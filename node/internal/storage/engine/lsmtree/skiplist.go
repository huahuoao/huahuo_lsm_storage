package lsmtree

import (
	"bytes"
	"math/rand"
)

// 跳表节点
type skipListNode struct {
	key   []byte          // 使用 []byte 作为键
	value []byte          // 使用 []byte 作为值
	next  []*skipListNode // 指向下一个节点的指针数组
}

// 跳表
type SkipList struct {
	head     *skipListNode
	level    int
	maxLevel int
	num      int // 跳表的节点数量
	size     int // 跳表中所有值的总字节数
}

// 创建新的跳表
func NewSkipList(maxLevel int) *SkipList {
	head := &skipListNode{next: make([]*skipListNode, maxLevel)}
	return &SkipList{head: head, level: 0, maxLevel: maxLevel, num: 0, size: 0}
}

// 随机生成层级
func randomLevel(maxLevel int) int {
	level := 1
	for rand.Float32() < 0.5 && level < maxLevel {
		level++
	}
	return level
}

// 插入节点
func (s *SkipList) Insert(key []byte, value []byte) {
	update := make([]*skipListNode, s.maxLevel)
	current := s.head

	// 查找插入位置
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	// 生成随机层级
	newLevel := randomLevel(s.maxLevel)
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel
	}

	// 创建新节点
	newNode := &skipListNode{key: key, value: value, next: make([]*skipListNode, newLevel)}
	for i := 0; i < newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	// 更新跳表的节点数量和大小
	s.num++
	s.size += len(key) + len(value) // 更新大小为 key 和 value 的字节数
}

// 查找节点
func (s *SkipList) Search(key []byte) ([]byte, bool) {
	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
	}
	current = current.next[0]
	if current != nil && bytes.Equal(current.key, key) {
		return current.value, true
	}
	return nil, false
}

// 删除节点
func (s *SkipList) Delete(key []byte) bool {
	update := make([]*skipListNode, s.maxLevel)
	current := s.head

	// 查找要删除的节点
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]

	// 如果找到了节点，进行删除
	if current != nil && bytes.Equal(current.key, key) {
		for i := 0; i < s.level; i++ {
			if update[i].next[i] != current {
				break
			}
			update[i].next[i] = current.next[i]
		}

		// 更新层级
		for s.level > 1 && s.head.next[s.level-1] == nil {
			s.level--
		}

		// 更新跳表的节点数量和大小
		s.num--
		s.size -= len(current.key) + len(current.value) // 更新大小为被删除节点的字节数
		return true
	}
	return false
}

// 跳表迭代器
type SkipListIterator struct {
	current *skipListNode
	list    *SkipList
}

// 创建迭代器
func (s *SkipList) Iterator() *SkipListIterator {
	return &SkipListIterator{
		current: s.head.next[0], // 从第一个实际节点开始
		list:    s,
	}
}

// 是否有下一个元素
func (it *SkipListIterator) HasNext() bool {
	return it.current != nil
}

// 获取下一个元素
func (it *SkipListIterator) Next() ([]byte, []byte) {
	if !it.HasNext() {
		return nil, nil
	}

	// 保存当前节点的键和值
	key := it.current.key
	value := it.current.value

	// 移动到下一个节点
	it.current = it.current.next[0]

	return key, value
}

// 重置迭代器
func (it *SkipListIterator) Reset() {
	it.current = it.list.head.next[0]
}
