package lsmtree

import (
	"github.com/krasun/rbytree"
)

// MemTable结构体相关说明。所有已刷新到预写日志（WAL）但尚未刷新到已排序文件的数据变更，都存储在内存中，以便更快地进行查找。
// 虽然可以直接使用红黑树（red-black）实例，但这个包装器以及额外的抽象层简化了后续的变更操作。
type memTable struct {
	data *rbytree.Tree
	// 插入到MemTable中的所有键和值的总大小，单位为字节（b在这里应该就是指字节）。
	b int
	n int //键值对数量
}

// newMemTable函数用于返回一个MemTable的新实例。
func newMemTable() *memTable {
	return &memTable{data: rbytree.New(), b: 0}
}

// put函数用于将键和值插入到表中。
func (mt *memTable) put(key, value []byte) error {
	// 尝试向红黑树中插入键值对，prev为之前该键对应的值（如果存在的话），exists表示该键是否原本已存在。
	prev, exists := mt.data.Put(key, value)
	// 如果键已存在，更新MemTable中数据总大小，减去之前值的长度并加上新值的长度。
	if exists {
		mt.b += -len(prev) + len(value)
	} else {
		// 如果键不存在，将键和新值的长度累加到总大小中。
		mt.b += len(key) + len(value)
		mt.n++
	}

	return nil
}

// get函数用于通过键来获取对应的值。
// 注意！对于内存中已被标记删除的键，该函数也会返回true（意味着可能获取到对应已删除键的相关信息，具体取决于底层实现逻辑）。
func (mt *memTable) get(key []byte) ([]byte, bool) {
	return mt.data.Get(key)
}

// delete函数用于在表中将键标记为已删除，但实际上并不会从数据结构中移除它（只是做一个删除标记的处理）。
func (mt *memTable) delete(key []byte) error {
	value, exists := mt.data.Put(key, nil)
	// 如果键原本不存在，将键的长度累加到总大小中。
	if !exists {
		mt.b += len(key)
	} else {
		// 如果键原本存在，减去对应值的长度，以更新总大小。
		mt.b -= len(value)
		mt.n--
	}

	return nil
}

// bytes函数用于返回插入到MemTable中的所有键和值的总大小，单位为字节。
func (mt *memTable) bytes() int {
	return mt.b
}

// clear函数用于清除所有数据，并重置总大小为0。
func (mt *memTable) clear() {
	mt.data = rbytree.New()
	mt.b = 0
}

// iterator函数用于返回MemTable的迭代器。该迭代器也会遍历已被标记删除的键，不过这些已删除键对应的值为nil。
func (mt *memTable) iterator() *memTableIterator {
	return &memTableIterator{mt.data.Iterator()}
}

// MemTable迭代器相关结构体定义。
type memTableIterator struct {
	it *rbytree.Iterator
}

// hasNext方法用于判断是否还有下一个元素，有则返回true。
func (it *memTableIterator) hasNext() bool {
	return it.it.HasNext()
}

// next方法用于返回当前的键和值，并将迭代器位置前进到下一个元素。
func (it *memTableIterator) next() ([]byte, []byte) {
	return it.it.Next()
}
