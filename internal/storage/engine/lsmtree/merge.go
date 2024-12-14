package lsmtree

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

// mergeDiskTables 函数用于合并磁盘表（索引为a和b的磁盘表），
// 并创建一个新的合并表（索引为b）。
// 索引a必须小于b，且代表更旧的表。
func mergeDiskTables(dbDir string, a, b int, sparseKeyDistance int) error {
	mergePrefix := "merge"
	aPrefix := strconv.Itoa(a) + "-"
	bPrefix := strconv.Itoa(b) + "-"

	// 获取索引为a的磁盘表数据文件的完整路径
	aPath := path.Join(dbDir, aPrefix+diskTableDataFileName)
	// 为索引为a的磁盘表数据文件实例化一个迭代器，如果失败则返回错误
	aIt, err := newDataFileIterator(aPath)
	if err != nil {
		return fmt.Errorf("为 %s 实例化迭代器失败: %w", aPath, err)
	}
	// 确保迭代器最终被关闭，释放相关资源
	defer aIt.close()

	// 获取索引为b的磁盘表数据文件的完整路径
	bPath := path.Join(dbDir, bPrefix+diskTableDataFileName)
	// 为索引为b的磁盘表数据文件实例化一个迭代器，如果失败则返回错误
	bIt, err := newDataFileIterator(bPath)
	if err != nil {
		return fmt.Errorf("为 %s 实例化迭代器失败: %w", bPath, err)
	}
	// 确保迭代器最终被关闭，释放相关资源
	defer bIt.close()

	// 创建一个新的磁盘表写入器，用于将合并后的数据写入磁盘，如果失败则返回错误
	w, err := newDiskTableWriter(dbDir, mergePrefix, sparseKeyDistance)
	if err != nil {
		return fmt.Errorf("实例化磁盘表写入器失败: %w", err)
	}

	// 使用迭代器合并磁盘表数据，如果失败则返回错误
	if err := merge(aIt, bIt, w); err != nil {
		return fmt.Errorf("合并磁盘表失败: %w", err)
	}

	// 关闭索引为a的磁盘表数据文件对应的迭代器，如果失败则返回错误
	if err := aIt.close(); err != nil {
		return fmt.Errorf("关闭 %s 的迭代器失败: %w", aPath, err)
	}

	// 关闭索引为b的磁盘表数据文件对应的迭代器，如果失败则返回错误
	if err := bIt.close(); err != nil {
		return fmt.Errorf("关闭 %s 的迭代器失败: %w", bPath, err)
	}

	// 删除索引为a和b的磁盘表，如果失败则返回错误
	if err := deleteDiskTables(dbDir, aPrefix, bPrefix); err != nil {
		return fmt.Errorf("删除磁盘表失败: %w", err)
	}

	// 将合并后的磁盘表重命名为索引为b的磁盘表的名称，如果失败则返回错误
	if err := renameDiskTable(dbDir, mergePrefix, bPrefix); err != nil {
		return fmt.Errorf("重命名合并后的磁盘表失败: %w", err)
	}

	return nil
}

// merge 函数用于合并来自a和b迭代器的键和值，并使用磁盘表写入器将它们写入磁盘表中。
func merge(aIt, bIt *dataFileIterator, w *diskTableWriter) error {
	var aKey, aValue, bKey, bValue []byte
	for {
		// 如果a的键为空且a迭代器还有下一个元素
		if aKey == nil && aIt.hasNext() {
			// 获取a迭代器的下一个键值对，如果失败则返回错误
			if k, v, err := aIt.next(); err != nil {
				return fmt.Errorf("获取a的下一个元素失败: %w", err)
			} else {
				aKey, aValue = k, v
			}
		}

		// 如果b的键为空且b迭代器还有下一个元素
		if bKey == nil && bIt.hasNext() {
			// 获取b迭代器的下一个键值对，如果失败则返回错误
			if k, v, err := bIt.next(); err != nil {
				return fmt.Errorf("获取b的下一个元素失败: %w", err)
			} else {
				bKey, bValue = k, v
			}
		}

		// 如果a和b的键都为空且a和b迭代器都没有下一个元素了，说明已经遍历完，返回nil表示结束
		if aKey == nil && bKey == nil && !aIt.hasNext() && !bIt.hasNext() {
			return nil
		}

		// 如果a和b的键都不为空
		if aKey != nil && bKey != nil {
			// 比较a和b的键
			cmp := bytes.Compare(aKey, bKey)

			// 如果键相等，由于b是更新的，可以丢弃a
			if cmp == 0 {
				// 将b的键值对写入磁盘表，如果写入失败则返回错误
				if err := w.write(bKey, bValue); err != nil {
					return fmt.Errorf("写入失败: %w", err)
				}
				// 将a和b的键值对都置为空，准备读取下一组
				aKey, aValue, bKey, bValue = nil, nil, nil, nil
			} else if cmp > 0 {
				// 如果a的键大于b的键
				// 将b的键值对写入磁盘表，并读取b的下一个键
				if err := w.write(bKey, bValue); err != nil {
					return fmt.Errorf("写入失败: %w", err)
				}
				bKey, bValue = nil, nil
			} else if cmp < 0 {
				// 如果a的键小于b的键
				// 将a的键值对写入磁盘表
				if err := w.write(aKey, aValue); err != nil {
					return fmt.Errorf("写入失败: %w", err)
				}
				aKey, aValue = nil, nil
			}
		} else if aKey != nil {
			// 如果只有a的键不为空，将a的键值对写入磁盘表，如果写入失败则返回错误
			if err := w.write(aKey, aValue); err != nil {
				return fmt.Errorf("写入失败: %w", err)
			}
			aKey, aValue = nil, nil
		} else {
			// 如果只有b的键不为空，将b的键值对写入磁盘表，如果写入失败则返回错误
			if err := w.write(bKey, bValue); err != nil {
				return fmt.Errorf("写入失败: %w", err)
			}
			bKey, bValue = nil, nil
		}
	}
}

// dataFileIterator 结构体允许对数据文件进行简单的迭代操作。
type dataFileIterator struct {
	dataFile *os.File
	key      []byte
	value    []byte
	end      bool
	closed   bool
}

// newDataFileIterator 函数用于实例化一个新的数据文件迭代器。
func newDataFileIterator(path string) (*dataFileIterator, error) {
	// 以只读模式打开指定路径的数据文件，如果失败则返回错误
	dataFile, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("打开数据文件 %s 失败: %w", path, err)
	}

	// 从数据文件中解码出键和值，如果读取失败且不是文件末尾错误，则返回错误
	key, value, err := decode(dataFile)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("读取失败: %w", err)
	}
	// 如果错误是文件末尾（io.EOF），则表示已经到文件末尾了
	end := err == io.EOF

	return &dataFileIterator{
		dataFile,
		key,
		value,
		end,
		false,
	}, nil
}

// hasNext 方法用于判断是否还有下一个元素。
func (it *dataFileIterator) hasNext() bool {
	return !it.end
}

// next 方法用于返回当前的键和值，并将迭代器位置前进到下一个元素。
func (it *dataFileIterator) next() ([]byte, []byte, error) {
	key, value := it.key, it.value

	// 从数据文件中读取下一个键值对，如果读取失败且不是文件末尾错误，则返回错误
	nextKey, nextValue, err := decode(it.dataFile)
	if err != nil && err != io.EOF {
		return nil, nil, fmt.Errorf("读取失败: %w", err)
	}
	// 如果错误是文件末尾（io.EOF），则标记迭代器已到末尾
	if err == io.EOF {
		it.end = true
	}

	// 更新迭代器的当前键和值为刚读取的下一组键值对
	it.key = nextKey
	it.value = nextValue

	return key, value, nil
}

// close 方法用于关闭相关联的数据文件。
func (it *dataFileIterator) close() error {
	if it.closed {
		return nil
	}

	// 关闭数据文件，如果关闭失败则返回错误
	if err := it.dataFile.Close(); err != nil {
		return fmt.Errorf("关闭失败: %w", err)
	}

	it.closed = true

	return nil
}
