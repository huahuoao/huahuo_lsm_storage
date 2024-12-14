package lsmtree

import (
	"fmt"
	"io"
	"os"
	"path"
)

// clearWAL关闭当前文件，并以截断模式打开新文件。
func clearWAL(dbDir string, wal *os.File) (*os.File, error) {
	// 拼接预写日志（WAL）文件的路径。
	walPath := path.Join(dbDir, walFileName)

	// 关闭当前的WAL文件，如果关闭失败则返回相应错误。
	if err := wal.Close(); err != nil {
		return nil, fmt.Errorf("failed to close the WAL file %s: %w", walPath, err)
	}

	// 以读写、创建、截断模式打开WAL文件，如果打开失败则返回相应错误。
	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open the file %s: %w", walPath, err)
	}

	return wal, nil
}

// appendToWAL将条目追加到WAL文件中。
func appendToWAL(wal *os.File, key []byte, value []byte) error {
	// 出于安全考虑，因为文件是以读写模式打开的，将文件指针定位到文件末尾，如果定位失败则返回相应错误。
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to the end: %w", err)
	}

	// 将键值对进行编码并写入文件，如果编码或写入失败则返回相应错误。
	if _, err := encode(key, value, wal); err != nil {
		return fmt.Errorf("failed to encode and write to the file: %w", err)
	}

	// 同步文件（将缓存中的数据刷写到磁盘等持久化存储），如果同步失败则返回相应错误。
	if err := wal.Sync(); err != nil {
		return fmt.Errorf("failed to sync the file: %w", err)
	}

	return nil
}

// loadMemTable从WAL文件中加载内存表（MemTable）。
func loadMemTable(wal *os.File) (*memTable, error) {
	// 出于安全考虑，因为文件是以读写模式打开的，将文件指针定位到文件开头，如果定位失败则返回相应错误。
	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to the beginning: %w", err)
	}

	// 创建一个新的内存表实例。
	memTable := newMemTable()
	for {
		// 从WAL文件中解码出键、值，如果读取或解码出现错误（非文件末尾错误）则返回相应错误，
		// 如果遇到文件末尾则返回已加载好的内存表实例。
		key, value, err := decode(wal)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return memTable, nil
		}

		// 如果值不为空，则将键值对插入内存表；如果值为空，则在内存表中根据键执行删除操作。
		if value != nil {
			memTable.put(key, value)
		} else {
			memTable.delete(key)
		}
	}
}
