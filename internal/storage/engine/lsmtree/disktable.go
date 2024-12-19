package lsmtree

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

const (
	// DiskTableNumFileName是包含最大磁盘表编号的磁盘表文件名。
	diskTableNumFileName = "maxdisktable"
	// DiskTableDataFileName是包含原始数据的磁盘表数据文件名。
	diskTableDataFileName = "data"
	// DiskTableIndexFileName是包含键以及指向数据文件中值的位置信息的磁盘表键文件名。
	diskTableIndexFileName = "index"
	// DiskTableSparseIndexFileName是磁盘表的稀疏索引文件名，它是索引文件中每64个条目的一个采样。
	diskTableSparseIndexFileName = "sparse"
	// newDiskTableFlag是用于创建新磁盘表文件（数据、索引和稀疏索引文件）时打开文件的标志。
	newDiskTableFlag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
)

// createDiskTable根据给定的内存表（MemTable）、在给定的目录下，使用给定的前缀创建一个磁盘表（DiskTable）。
func createDiskTable(memTable *memTable, dbDir string, index, sparseKeyDistance int) error {
	prefix := strconv.Itoa(index) + "-"

	w, err := newDiskTableWriter(dbDir, prefix, sparseKeyDistance)
	if err != nil {
		return fmt.Errorf("failed to create disk table writer: %w", err)
	}

	for it := memTable.iterator(); it.hasNext(); {
		key, value := it.next()
		if err := w.write(key, value); err != nil {
			return fmt.Errorf("failed to write to disk table %d: %w", index, err)
		}
	}

	if err := w.sync(); err != nil {
		return fmt.Errorf("failed to sync disk table: %w", err)
	}

	if err := w.close(); err != nil {
		return fmt.Errorf("failed to close disk table: %w", err)
	}

	return nil
}

// searchInDiskTables通过遍历目录中的所有磁盘表，根据给定的键在磁盘表中查找对应的值。
func searchInDiskTables(dbDir string, maxIndex int, key []byte) ([]byte, bool, error) {
	for index := maxIndex; index >= 0; index-- {
		value, exists, err := searchInDiskTable(dbDir, index, key)
		if err != nil {
			return nil, false, fmt.Errorf("failed to search in disk table with index %d: %w", index, err)
		}

		if exists {
			return value, exists, nil
		}
	}

	return nil, false, nil
}

// searchInDiskTable在给定的磁盘表中查找给定的键。
func searchInDiskTable(dbDir string, index int, key []byte) ([]byte, bool, error) {
	prefix := strconv.Itoa(index) + "-"

	sparseIndexPath := path.Join(dbDir, prefix+diskTableSparseIndexFileName)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open sparse index file: %w", err)
	}
	defer sparseIndexFile.Close()

	from, to, ok, err := searchInSparseIndex(sparseIndexFile, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in sparse index file %s: %w", sparseIndexPath, err)
	}
	if !ok {
		return nil, false, nil
	}

	indexPath := path.Join(dbDir, prefix+diskTableIndexFileName)
	indexFile, err := os.OpenFile(indexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open index file: %w", err)
	}
	defer indexFile.Close()

	offset, ok, err := searchInIndex(indexFile, from, to, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in index file %s: %w", indexPath, err)
	}
	if !ok {
		return nil, false, nil
	}

	dataPath := path.Join(dbDir, prefix+diskTableDataFileName)
	dataFile, err := os.OpenFile(dataPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	value, ok, err := searchInDataFile(dataFile, offset, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in data file %s: %w", dataPath, err)
	}

	if err := sparseIndexFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close sparse index file: %w", err)
	}

	if err := indexFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close index file: %w", err)
	}

	if err := dataFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close data file: %w", err)
	}

	return value, ok, nil
}

// searchInDataFile从给定的偏移量开始，在数据文件中根据键查找对应的值。
// 偏移量必须始终指向记录的开头。
func searchInDataFile(r io.ReadSeeker, offset int, searchKey []byte) ([]byte, bool, error) {
	if _, err := r.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r)
		if err != nil && err != io.EOF {
			return nil, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return nil, false, nil
		}

		if bytes.Equal(key, searchKey) {
			return value, true, nil
		}
	}
}

// searchInIndex在指定范围内的索引文件中查找键。
func searchInIndex(r io.ReadSeeker, from, to int, searchKey []byte) (int, bool, error) {
	if _, err := r.Seek(int64(from), io.SeekStart); err != nil {
		return 0, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r)
		if err != nil && err != io.EOF {
			return 0, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return 0, false, nil
		}
		offset := decodeInt(value)

		if bytes.Equal(key, searchKey) {
			return offset, true, nil
		}

		if to > from {
			current, err := r.Seek(0, io.SeekCurrent)
			if err != nil {
				return 0, false, fmt.Errorf("failed to seek: %w", err)
			}

			if current > int64(to) {
				return 0, false, nil
			}
		}
	}
}

// searchInSparseIndex查找键所在的范围。
func searchInSparseIndex(r io.Reader, searchKey []byte) (int, int, bool, error) {
	from := -1
	for {
		key, value, err := decode(r)
		if err != nil && err != io.EOF {
			return 0, 0, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return from, 0, from != -1, nil
		}
		offset := decodeInt(value)

		cmp := bytes.Compare(key, searchKey)
		if cmp == 0 {
			return offset, offset, true, nil
		} else if cmp < 0 {
			from = offset
		} else if cmp > 0 {
			if from == -1 {
				// 如果稀疏索引中的第一个键大于查找的键，意味着不存在该键
				return 0, 0, false, nil
			} else {
				return from, offset, true, nil
			}
		}
	}
}

// renameDiskTable重命名磁盘表的相关文件，包括数据、索引和稀疏索引文件。
func renameDiskTable(dbDir string, oldPrefix, newPrefix string) error {
	if err := os.Rename(path.Join(dbDir, oldPrefix+diskTableDataFileName), path.Join(dbDir, newPrefix+diskTableDataFileName)); err != nil {
		return fmt.Errorf("failed to rename data file: %w", err)
	}

	if err := os.Rename(path.Join(dbDir, oldPrefix+diskTableIndexFileName), path.Join(dbDir, newPrefix+diskTableIndexFileName)); err != nil {
		return fmt.Errorf("failed to rename index file: %w", err)
	}

	if err := os.Rename(path.Join(dbDir, oldPrefix+diskTableSparseIndexFileName), path.Join(dbDir, newPrefix+diskTableSparseIndexFileName)); err != nil {
		return fmt.Errorf("failed to rename sparse index file: %w", err)
	}

	return nil
}

// deleteDiskTables删除磁盘表的相关文件，包括数据、索引和稀疏索引文件。
func deleteDiskTables(dbDir string, prefixes ...string) error {
	for _, prefix := range prefixes {
		dataPath := path.Join(dbDir, prefix+diskTableDataFileName)
		if err := os.Remove(dataPath); err != nil {
			return fmt.Errorf("failed to remove data file %s: %w", dataPath, err)
		}

		indexPath := path.Join(dbDir, prefix+diskTableIndexFileName)
		if err := os.Remove(indexPath); err != nil {
			return fmt.Errorf("failed to remove data file %s: %w", indexPath, err)
		}

		sparseIndexPath := path.Join(dbDir, prefix+diskTableSparseIndexFileName)
		if err := os.Remove(sparseIndexPath); err != nil {
			return fmt.Errorf("failed to remove data file %s: %w", sparseIndexPath, err)
		}
	}

	return nil
}

// diskTableWriter是磁盘表的一个简单抽象，仅用于写入操作相关的功能。
type diskTableWriter struct {
	dataFile        *os.File
	indexFile       *os.File
	sparseIndexFile *os.File

	sparseKeyDistance int

	keyNum, dataPos, indexPos int
}

// newDiskTableWriter返回一个新的diskTableWriter实例。
func newDiskTableWriter(dbDir, prefix string, sparseKeyDistance int) (*diskTableWriter, error) {
	dataPath := path.Join(dbDir, prefix+diskTableDataFileName)
	dataFile, err := os.OpenFile(dataPath, newDiskTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", dataPath, err)
	}

	indexPath := path.Join(dbDir, prefix+diskTableIndexFileName)
	indexFile, err := os.OpenFile(indexPath, newDiskTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", indexPath, err)
	}

	sparseIndexPath := path.Join(dbDir, prefix+diskTableSparseIndexFileName)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, newDiskTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open sparse index file %s: %w", sparseIndexPath, err)
	}

	return &diskTableWriter{
		dataFile:          dataFile,
		indexFile:         indexFile,
		sparseIndexFile:   sparseIndexFile,
		sparseKeyDistance: sparseKeyDistance,
		keyNum:            0,
		dataPos:           0,
		indexPos:          0,
	}, nil
}

// write将键和值写入磁盘表的相关文件，即数据、索引和稀疏索引文件。
func (w *diskTableWriter) write(key, value []byte) error {
	dataBytes, err := encode(key, value, w.dataFile)
	if err != nil {
		return fmt.Errorf("failed to write to the data file: %w", err)
	}

	indexBytes, err := encodeKeyOffset(key, w.dataPos, w.indexFile)
	if err != nil {
		return fmt.Errorf("failed to write to the index file: %w", err)
	}

	if w.keyNum%w.sparseKeyDistance == 0 {
		if _, err := encodeKeyOffset(key, w.indexPos, w.sparseIndexFile); err != nil {
			return fmt.Errorf("failed to write to the file: %w", err)
		}
	}

	w.dataPos += dataBytes
	w.indexPos += indexBytes
	w.keyNum++

	return nil
}

// sync将所有已写入的内容提交到稳定存储中。
func (w *diskTableWriter) sync() error {
	if err := w.dataFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync data file: %w", err)
	}

	if err := w.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %w", err)
	}

	if err := w.sparseIndexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync sparse index file: %w", err)
	}

	return nil
}

// close关闭与磁盘表相关联的所有文件。
func (w *diskTableWriter) close() error {
	if err := w.dataFile.Close(); err != nil {
		return fmt.Errorf("failed to close data file: %w", err)
	}

	if err := w.indexFile.Close(); err != nil {
		return fmt.Errorf("failed to close index file: %w", err)
	}

	if err := w.sparseIndexFile.Close(); err != nil {
		return fmt.Errorf("failed to close sparse index file: %w", err)
	}

	return nil
}

// updateDiskTableMeta更新当前最大磁盘表编号。
func updateDiskTableMeta(dbDir string, num, max int) error {
	filePath := path.Join(dbDir, diskTableNumFileName)
	if err := os.WriteFile(filePath, encodeIntPair(num, max), 0600); err != nil {
		return fmt.Errorf("failed to write %s: %w", filePath, err)
	}

	return nil
}

// readDiskTableMeta读取并返回磁盘表编号以及最大索引值。
func readDiskTableMeta(dbDir string) (int, int, error) {
	filePath := path.Join(dbDir, diskTableNumFileName)
	data, err := os.ReadFile(filePath)
	if err != nil && !os.IsNotExist(err) {
		return 0, 0, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	if err != nil && os.IsNotExist(err) {
		return 0, -1, nil
	}

	num, max := decodeIntPair(data)

	return num, max, nil
}
