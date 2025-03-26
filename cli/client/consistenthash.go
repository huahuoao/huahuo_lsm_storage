package client

import (
	"crypto/md5"
	"errors"
	"sort"
	"strconv"
	"sync"
)

var (
	instance *HashRing
	once     sync.Once
)

// GetRing 返回全局唯一的哈希环单例（懒汉式线程安全）
func GetRing() *HashRing {
	once.Do(func() {
		instance = &HashRing{
			replicas: 160, // 默认160个虚拟节点
			hashMap:  make(map[int64]string),
		}
	})
	return instance
}

// computeMD5 computes the MD5 hash of the given string.
func computeMD5(s string) [16]byte {
	return md5.Sum([]byte(s))
}

// hash extracts a specific 32-bit integer from the digest (Ketama feature).
func hash(digest *[16]byte, h int) int64 {
	k := ((int64((*digest)[3+h*4]) & 0xFF) << 24) |
		((int64((*digest)[2+h*4]) & 0xFF) << 16) |
		((int64((*digest)[1+h*4]) & 0xFF) << 8) |
		(int64((*digest)[h*4]) & 0xFF)
	return k
}

// HashRing represents the structure of a consistent hash ring.
type HashRing struct {
	replicas int              // Number of virtual nodes per physical node
	keys     []int64          // Sorted hash values
	hashMap  map[int64]string // Mapping from hash values to physical node names
}

// NewRing creates a new hash ring.
func NewRing() *HashRing {
	m := &HashRing{
		replicas: 160, // Number of virtual nodes
		hashMap:  make(map[int64]string),
	}
	return m
}

// Add adds new physical nodes to the hash ring.
func (m *HashRing) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			virtualNodeKey := key + strconv.Itoa(i)
			digest := computeMD5(virtualNodeKey)
			for j := 0; j < 4; j++ {
				hash := hash(&digest, j)
				m.keys = append(m.keys, hash)
				m.hashMap[hash] = key
			}
		}
	}
	sort.Slice(m.keys, func(i, j int) bool {
		return m.keys[i] < m.keys[j]
	})
}

// Get retrieves the closest physical node for the given key.
func (m *HashRing) Get(key string) (string, error) {
	if len(m.keys) == 0 {
		return "", nil
	}
	if len(m.hashMap) == 0 {
		return "", errors.New("no node available!")
	}
	digest := computeMD5(key)
	hash := hash(&digest, 0)
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}
	return m.hashMap[m.keys[idx]], nil
}

func (m *HashRing) Remove(node string) {
	// 遍历哈希映射，移除与目标节点相关的所有虚拟节点
	for hashValue, physicalNode := range m.hashMap {
		if physicalNode == node {
			delete(m.hashMap, hashValue)
		}
	}

	// 重建 keys 列表
	newKeys := make([]int64, 0, len(m.keys))
	for _, key := range m.keys {
		if m.hashMap[key] != node {
			newKeys = append(newKeys, key)
		}
	}

	// 更新 keys 列表并重新排序
	m.keys = newKeys
	sort.Slice(m.keys, func(i, j int) bool {
		return m.keys[i] < m.keys[j]
	})
}
