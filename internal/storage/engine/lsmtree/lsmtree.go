package lsmtree

import (
	"errors"
	"fmt"
	"github.com/huahuoao/lsm-core/internal/utils"
	"github.com/seiflotfy/cuckoofilter"
	"math"
	"os"
	"path"
	"strconv"
	"sync"
)

const (
	// MaxKeySize 是允许的最大键大小。
	// 该大小是硬编码的，不能更改，因为这会影响编码特性。
	MaxKeySize = math.MaxUint16
	// MaxValueSize 是允许的最大值大小。
	// 该大小是硬编码的，不能更改，因为这会影响编码特性。
	MaxValueSize = math.MaxUint16
)

const (
	// WAL 文件名。
	walFileName = "wal.db"
	// 默认 MemTable 表阈值。
	defaultMemTableThreshold = 16000 // 16 kB
	// 稀疏索引中键之间的默认距离。
	defaultSparseKeyDistance = 128
	// 默认 DiskTable 数量阈值。
	defaultDiskTableNumThreshold = 3
)

var (
	// ErrKeyRequired 当放入零长度键或 nil 时返回。
	ErrKeyRequired = errors.New("key required")
	// ErrValueRequired 当放入零长度值或 nil 时返回。
	ErrValueRequired = errors.New("value required")
	// ErrKeyTooLarge 当放入的键大于 MaxKeySize 时返回。
	ErrKeyTooLarge = errors.New("key too large")
	// ErrValueTooLarge 当放入的值大于 MaxValueSize 时返回。
	ErrValueTooLarge = errors.New("value too large")
)

// LSMTree (https://en.wikipedia.org/wiki/Log-structured_merge-tree)
// 是针对存储数据在文件中的日志结构合并树实现。
// 该实现不是 goroutine 安全的！如果需要，确保对树的访问是同步的。
type LSMTree struct {
	// 存储 LSM 树文件的目录的路径，
	// 必须为树的每个实例提供专用目录。
	dbDir string

	// 在执行任何写操作之前，
	// 它会写入写前日志（WAL），然后才应用。
	wal *os.File

	// 它指向磁盘上最新创建的 DiskTable。
	// MemTable 被刷新后，索引会更新。
	// 默认 -1 表示没有 DiskTable。
	maxDiskTableIndex int

	// 持久存储中已刷新和合并的磁盘表的当前数量。
	diskTableNum int

	// 所有已刷新到 WAL 但未刷新到已排序文件中的更改
	// 可读写的内存表
	memTable *memTable
	//不可读内存表
	immutableMemtables []*memTable
	//不可读内存表的数量最大限制
	immutableMemtableMaxNum int
	// 如果 MemTable 的大小（以字节为单位）超过阈值，
	// 必须将其刷新到文件系统。
	memTableThreshold int

	// 如果 DiskTable 的数量超过阈值，
	// 磁盘表必须被合并以减少它。
	diskTableNumThreshold int

	// 稀疏索引中键之间的距离。
	sparseKeyDistance int
	// 不可变表的合并写入互斥锁
	mu sync.RWMutex
	// 布谷鸟过滤器
	cuckooFilters map[int]*cuckoo.Filter
}

// MemTableThreshold 为 LSMTree 设置 memTableThreshold。
// 如果 MemTable 的大小（以字节为单位）超过阈值，必须
// 将其刷新到文件系统。
func MemTableThreshold(memTableThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.memTableThreshold = memTableThreshold
	}
}

// SparseKeyDistance 为 LSMTree 设置 sparseKeyDistance。
// 稀疏索引中键之间的距离。
func SparseKeyDistance(sparseKeyDistance int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.sparseKeyDistance = sparseKeyDistance
	}
}

// DiskTableNumThreshold 为 LSMTree 设置 diskTableNumThreshold。
// 如果 DiskTable 的数量超过阈值，磁盘表必须
// 被合并以减少它。
func DiskTableNumThreshold(diskTableNumThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.diskTableNumThreshold = diskTableNumThreshold
	}
}

// Open 打开数据库。只有一个树的实例可以
// 读取和写入该目录。
func Open(dbDir string, options ...func(*LSMTree)) (*LSMTree, error) {
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory %s does not exist", dbDir)
	}

	walPath := path.Join(dbDir, walFileName)
	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", walPath, err)
	}

	memTable, err := loadMemTable(wal)
	if err != nil {
		return nil, fmt.Errorf("failed to load entries from %s: %w", walPath, err)
	}

	diskTableNum, maxDiskTableIndex, err := readDiskTableMeta(dbDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read disk table meta: %w", err)
	}

	t := &LSMTree{
		wal:                     wal,
		memTable:                memTable,
		dbDir:                   dbDir,
		maxDiskTableIndex:       maxDiskTableIndex,
		memTableThreshold:       defaultMemTableThreshold,
		sparseKeyDistance:       defaultSparseKeyDistance,
		diskTableNum:            diskTableNum,
		diskTableNumThreshold:   defaultDiskTableNumThreshold,
		immutableMemtableMaxNum: 4,
	}
	for _, option := range options {
		option(t)
	}

	return t, nil
}
func (t *LSMTree) refreshMemTable() {
	t.memTable = newMemTable()
}

// Close 关闭所有分配的资源。
func (t *LSMTree) Close() error {
	if err := t.wal.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", t.wal.Name(), err)
	}

	return nil
}

// Put 将键放入数据库中。
func (t *LSMTree) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) == 0 {
		return ErrValueRequired
	} else if uint64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	if err := appendToWAL(t.wal, key, value); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", t.wal.Name(), err)
	}

	t.memTable.put(key, value)

	if t.memTable.bytes() >= t.memTableThreshold {
		// 当前 Memtable 已经达到了设定的大小阈值
		// 将当前的 Memtable 转为只读并添加到 immutableMemtables
		t.immutableMemtables = append(t.immutableMemtables, t.memTable)
		// 创建一个新的 Memtable 来继续接收写入
		t.refreshMemTable()
	}
	//不可变内存表数量超过限制的时候进行合并，写入磁盘
	if len(t.immutableMemtables) >= t.immutableMemtableMaxNum {
		err := t.compactImmutableMemtable()
		if err != nil {
			return err
		}
	}
	if t.diskTableNum >= t.diskTableNumThreshold {
		oldest := t.maxDiskTableIndex - t.diskTableNum + 1
		merged := false
		updateIndexMap := make(map[string]string)
		// 遍历所有可能的相邻表对
		for i := oldest; i < t.maxDiskTableIndex; i++ {
			a := i
			b := i + 1

			aPath := path.Join(t.dbDir, fmt.Sprintf("%d-%s", a, diskTableDataFileName))
			bPath := path.Join(t.dbDir, fmt.Sprintf("%d-%s", b, diskTableDataFileName))

			aSize, err := utils.GetFileSize(aPath)
			if err != nil {
				continue // 文件不存在，跳过
			}

			bSize, err := utils.GetFileSize(bPath)
			if err != nil {
				continue
			}

			// 检查总大小是否超过64MB
			if aSize+bSize > 2*1024*1024 {
				aPrefix := strconv.Itoa(a) + "-"
				bPrefix := strconv.Itoa(b) + "-"
				updateIndexMap[aPrefix] = bPrefix
				continue
			}

			// 合并表对
			if err := mergeDiskTables(t.dbDir, a, b, t.sparseKeyDistance); err != nil {
				return fmt.Errorf("failed to merge disk tables %d and %d: %w", a, b, err)
			}

			// 更新元数据
			newDiskTableNum := t.diskTableNum - 1
			if err := updateDiskTableMeta(t.dbDir, newDiskTableNum, t.maxDiskTableIndex); err != nil {
				return fmt.Errorf("failed to update disk table meta: %w", err)
			}
			for aPrefix, bPrefix := range updateIndexMap {
				err := renameDiskTable(t.dbDir, aPrefix, bPrefix)
				if err != nil {
					return err
				}
			}
			t.diskTableNum = newDiskTableNum
			merged = true
			break
		}

		if !merged {
			return fmt.Errorf("all adjacent disk table pairs exceed 64MB and cannot be merged")
		}
	}

	return nil
}

func (t *LSMTree) compactImmutableMemtable() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	merged := NewSkipList(16)
	for _, list := range t.immutableMemtables {
		l := list.data
		current := l.head.next[0]
		for current != nil {
			merged.Insert(current.key, current.value)
			current = current.next[0]
		}
	}
	err := t.flushMemTable(&memTable{data: merged})
	if err != nil {
		return err
	}
	t.immutableMemtables = []*memTable{}
	return nil
}

// Get 从数据库中获取键的值。
func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, exists := t.memTable.get(key)
	if exists {
		return value, value != nil, nil
	}
	value, exists, err := t.SearchInImmutableMemtable(key)
	if exists {
		return value, value != nil, nil
	}
	value, exists, err = searchInDiskTables(t.dbDir, t.maxDiskTableIndex, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in DiskTables: %w", err)
	}

	return value, exists, nil
}
func (t *LSMTree) SearchInImmutableMemtable(key []byte) ([]byte, bool, error) {
	tables := t.immutableMemtables
	for _, table := range tables {
		value, exists := table.get(key)
		if exists {
			return value, value != nil, nil
		}
	}
	return nil, false, nil
}

// Delete 根据键从数据库中删除值。
func (t *LSMTree) Delete(key []byte) error {
	if err := appendToWAL(t.wal, key, nil); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", t.wal.Name(), err)
	}

	t.memTable.delete(key)

	return nil
}

// flushMemTable 将当前的 MemTable 刷新到磁盘并清除它。
// 该函数期望在同步块中运行，
// 因此它不使用任何同步机制。
func (t *LSMTree) flushMemTable(table *memTable) error {
	newDiskTableNum := t.diskTableNum + 1
	newDiskTableIndex := t.maxDiskTableIndex + 1

	if err := createDiskTable(table, t.dbDir, newDiskTableIndex, t.sparseKeyDistance); err != nil {
		return fmt.Errorf("failed to create disk table %d: %w", newDiskTableIndex, err)
	}

	if err := updateDiskTableMeta(t.dbDir, newDiskTableNum, newDiskTableIndex); err != nil {
		return fmt.Errorf("failed to update max disk table index %d: %w", newDiskTableIndex, err)
	}

	newWAL, err := clearWAL(t.dbDir, t.wal)
	if err != nil {
		return fmt.Errorf("failed to clear the WAL file: %w", err)
	}

	t.wal = newWAL
	t.diskTableNum = newDiskTableNum
	t.maxDiskTableIndex = newDiskTableIndex

	return nil
}

// PrintStatus 打印当前树的状态，包括 memTable 和 immutableMemtables 的信息。
func (t *LSMTree) PrintStatus() {
	fmt.Printf("MemTable: n:%d, b:%d kb:\n", t.memTable.data.num, t.memTable.bytes()/1024)
	// 打印不可读内存表的状态
	totalImmutableSize := 0
	totalImmutableCount := 0
	for i, immutableTable := range t.immutableMemtables {
		immutableSize := immutableTable.bytes() // 获取不可读内存表的字节数
		immutableCount := immutableTable.size() // 获取不可读内存表的 KV 数量
		totalImmutableSize += immutableSize
		totalImmutableCount += immutableCount
		fmt.Printf("immutableTable %d n:%d, b:%d kb:\n", i, immutableCount, immutableSize/1024)

	}
}
