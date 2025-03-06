package lsmtree

import (
	"testing"
)

func TestSkipList(t *testing.T) {
	skipList := NewSkipList(16)

	// 测试插入
	skipList.Insert([]byte("1"), []byte("one"))
	skipList.Insert([]byte("2"), []byte("two"))
	skipList.Insert([]byte("3"), []byte("three"))

	// 测试查找
	if value, found := skipList.Search([]byte("1")); !found || string(value) != "one" {
		t.Errorf("Expected to find key '1' with value 'one', got %v", value)
	}
	if value, found := skipList.Search([]byte("2")); !found || string(value) != "two" {
		t.Errorf("Expected to find key '2' with value 'two', got %v", value)
	}
	if value, found := skipList.Search([]byte("3")); !found || string(value) != "three" {
		t.Errorf("Expected to find key '3' with value 'three', got %v", value)
	}
	if _, found := skipList.Search([]byte("4")); found {
		t.Error("Expected not to find key '4'")
	}

	// 测试删除
	if deleted := skipList.Delete([]byte("2")); !deleted {
		t.Error("Expected to delete key '2'")
	}
	if _, found := skipList.Search([]byte("2")); found {
		t.Error("Expected not to find key '2' after deletion")
	}

	// 测试节点数量和大小
	if skipList.num != 2 {
		t.Errorf("Expected num to be 2, got %d", skipList.num)
	}

}
