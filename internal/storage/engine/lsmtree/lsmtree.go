package lsmtree

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
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
	defaultMemTableThreshold = 64000 // 64 kB
	// 稀疏索引中键之间的默认距离。
	defaultSparseKeyDistance = 128
	// 默认 DiskTable 数量阈值。
	defaultDiskTableNumThreshold = 10
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
	// 存储在内存中以便于更快的查找。
	memTable *memTable

	// 如果 MemTable 的大小（以字节为单位）超过阈值，
	// 必须将其刷新到文件系统。
	memTableThreshold int

	// 如果 DiskTable 的数量超过阈值，
	// 磁盘表必须被合并以减少它。
	diskTableNumThreshold int

	// 稀疏索引中键之间的距离。
	sparseKeyDistance int
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
		wal:                   wal,
		memTable:              memTable,
		dbDir:                 dbDir,
		maxDiskTableIndex:     maxDiskTableIndex,
		memTableThreshold:     defaultMemTableThreshold,
		sparseKeyDistance:     defaultSparseKeyDistance,
		diskTableNum:          diskTableNum,
		diskTableNumThreshold: defaultDiskTableNumThreshold,
	}
	for _, option := range options {
		option(t)
	}

	return t, nil
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
		if err := t.flushMemTable(); err != nil {
			return fmt.Errorf("failed to flush MemTable: %w", err)
		}
	}

	if t.diskTableNum >= t.diskTableNumThreshold {
		oldest := t.maxDiskTableIndex - t.diskTableNum + 1
		if err := mergeDiskTables(t.dbDir, oldest, oldest+1, t.sparseKeyDistance); err != nil {
			return fmt.Errorf("failed to merge disk tables: %w", err)
		}

		newDiskTableNum := t.diskTableNum - 1
		if err := updateDiskTableMeta(t.dbDir, newDiskTableNum, t.maxDiskTableIndex); err != nil {
			return fmt.Errorf("failed to update disk table meta: %w", err)
		}

		t.diskTableNum--
	}

	return nil
}

// Get 从数据库中获取键的值。
func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, exists := t.memTable.get(key)
	if exists {
		return value, value != nil, nil
	}

	value, exists, err := searchInDiskTables(t.dbDir, t.maxDiskTableIndex, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in DiskTables: %w", err)
	}

	return value, exists, nil
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
func (t *LSMTree) flushMemTable() error {
	newDiskTableNum := t.diskTableNum + 1
	newDiskTableIndex := t.maxDiskTableIndex + 1

	if err := createDiskTable(t.memTable, t.dbDir, newDiskTableIndex, t.sparseKeyDistance); err != nil {
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
	t.memTable.clear()
	t.diskTableNum = newDiskTableNum
	t.maxDiskTableIndex = newDiskTableIndex

	return nil
}
