package lsmtree

import (
	"os"
	"path"
	"testing"
)

// 测试清空WAL文件的功能
func TestClearWAL(t *testing.T) {
	// 创建一个临时目录用于测试
	tmpDir := t.TempDir()

	// 创建一个WAL文件以供测试
	walFile, err := os.OpenFile(path.Join(tmpDir, "wal.log"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("创建WAL文件失败: %v", err)
	}

	// 测试正常路径
	wal, err := clearWAL(tmpDir, walFile)
	if err != nil {
		t.Fatalf("清空WAL文件失败: %v", err)
	}
	if wal == nil {
		t.Fatal("返回的WAL文件为nil")
	}

	// 关闭WAL文件
	if err := wal.Close(); err != nil {
		t.Fatalf("关闭WAL文件失败: %v", err)
	}

	// 测试关闭WAL文件失败的情况
	_, err = clearWAL(tmpDir, nil)
	if err == nil {
		t.Fatal("预期应返回错误，但没有错误")
	}

	// 测试打开WAL文件失败的情况
	err = os.Remove(path.Join(tmpDir, "wal.log")) // 删除文件，以便下次打开将失败
	if err != nil {
		t.Fatalf("删除WAL文件失败: %v", err)
	}
	_, err = clearWAL(tmpDir, walFile)
	if err == nil {
		t.Fatal("预期应返回错误，但没有错误")
	}
}

// 测试追加条目到WAL文件
func TestAppendToWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建WAL文件
	walFile, err := os.OpenFile(path.Join(tmpDir, "wal.log"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("创建WAL文件失败: %v", err)
	}
	defer walFile.Close()

	// 正常的写入操作
	if err := appendToWAL(walFile, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("追加条目失败: %v", err)
	}

	// 创建新的WAL文件以测试错误情况
	walFile.Close()
	walFile, err = os.OpenFile(path.Join(tmpDir, "wal.log"), os.O_RDONLY, 0600) // 以只读模式打开
	if err != nil {
		t.Fatalf("创建WAL文件失败: %v", err)
	}

	// 尝试在只读文件中追加条目，期望会失败
	err = appendToWAL(walFile, []byte("key2"), []byte("value2"))
	if err == nil {
		t.Fatal("预期应返回错误，但没有错误")
	}
}

// 测试加载内存表的功能
func TestLoadMemTable(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建WAL文件并写入数据
	walFile, err := os.OpenFile(path.Join(tmpDir, "wal.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("创建WAL文件失败: %v", err)
	}
	defer walFile.Close()

	if err := appendToWAL(walFile, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("追加条目失败: %v", err)
	}
	if err := appendToWAL(walFile, []byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("追加条目失败: %v", err)
	}

	// 测试加载内存表
	memTable, err := loadMemTable(walFile)
	if err != nil {
		t.Fatalf("加载内存表失败: %v", err)
	}
	if memTable == nil {
		t.Fatal("返回的内存表为nil")
	}

	// 测试加载空WAL文件的情况
	emptyFile, err := os.OpenFile(path.Join(tmpDir, "empty_wal.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("创建空WAL文件失败: %v", err)
	}
	defer emptyFile.Close()

	memTable, err = loadMemTable(emptyFile)
	if err != nil {
		t.Fatalf("加载空内存表失败: %v", err)
	}
	if memTable != nil {
		t.Fatal("加载空内存表应该返回nil")
	}
}
