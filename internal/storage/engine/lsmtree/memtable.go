package lsmtree

// MemTable结构体相关说明。所有已刷新到预写日志（WAL）但尚未刷新到已排序文件的数据变更，都存储在内存中，以便更快地进行查找。
// 虽然可以直接使用红黑树（red-black）实例，但这个包装器以及额外的抽象层简化了后续的变更操作。
type memTable struct {
	data *SkipList
	// 插入到MemTable中的所有键和值的总大小，单位为字节（b在这里应该就是指字节）。
	b int
	n int //键值对数量
}

// newMemTable函数用于返回一个MemTable的新实例。
func newMemTable() *memTable {
	return &memTable{data: NewSkipList(16), n: 0, b: 0}
}

// put函数用于将键和值插入到表中。
func (mt *memTable) put(key, value []byte) error {
	mt.data.Insert(key, value)
	return nil
}

// get函数用于通过键来获取对应的值。
func (mt *memTable) get(key []byte) ([]byte, bool) {
	return mt.data.Search(key)
}

// delete
func (mt *memTable) delete(key []byte) error {
	mt.data.Delete(key)
	return nil
}

// bytes函数用于返回插入到MemTable中的所有键和值的总大小，单位为字节。
func (mt *memTable) bytes() int {
	return mt.data.size
}

func (mt *memTable) size() int {
	return mt.data.num
}

// clear函数用于清除所有数据，并重置总大小为0。
func (mt *memTable) clear() {
	mt.data = NewSkipList(16)
	mt.b = 0
}

// iterator函数用于返回MemTable的迭代器。该迭代器也会遍历已被标记删除的键，不过这些已删除键对应的值为nil。
func (mt *memTable) iterator() *memTableIterator {
	return &memTableIterator{mt.data.Iterator()}
}

// MemTable迭代器相关结构体定义。
type memTableIterator struct {
	it *SkipListIterator
}

// hasNext方法用于判断是否还有下一个元素，有则返回true。
func (it *memTableIterator) hasNext() bool {
	return it.it.HasNext()
}

// next方法用于返回当前的键和值，并将迭代器位置前进到下一个元素。
func (it *memTableIterator) next() ([]byte, []byte) {
	return it.it.Next()
}
