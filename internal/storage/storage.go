package storage

import (
	"github.com/huahuoao/lsm-core/internal/storage/engine/lsmtree"
	"os"
)

var h *Hbase

type Hbase struct {
	tree *lsmtree.LSMTree
}

func GetClient() *Hbase {
	if h == nil {
		h, _ = NewHbaseClient()
	}
	return h
}
func NewHbaseClient() (*Hbase, error) {
	h := &Hbase{}
	err := h.initTree()
	if err != nil {
		return nil, err
	}
	return h, err
}

func (h *Hbase) initTree() error {
	walPath := lsmtree.GetDatabaseSourcePath()
	_ = os.MkdirAll(walPath, 0700)
	tree, err := lsmtree.Open(walPath)
	if err != nil {
		return err
	}
	h.tree = tree
	return nil
}

func (h *Hbase) Get(key []byte) ([]byte, bool) {
	if h.tree == nil {
		err := h.initTree()
		if err != nil {
			return nil, false
		}
	}

	value, exists, err := h.tree.Get(key)
	if err != nil {
		return nil, false
	}
	return value, exists
}

func (h *Hbase) Put(key []byte, value []byte) error {
	if h.tree == nil {
		err := h.initTree()
		if err != nil {
			return nil
		}
	}
	err := h.tree.Put(key, value)
	if err != nil {
		return err
	}
	return nil
}
