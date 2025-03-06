package lsmtree

const (
	databaseSourcePath = "/lsm_huahuo/"
	// 默认 MemTable 表阈值。
	defaultMemTableThreshold = 16000 // 16 kB
	// 稀疏索引中键之间的默认距离。
	defaultSparseKeyDistance = 128
	// 默认 SSTable 数量阈值。
	defaultDiskTableNumThreshold = 10
	// 默认单个SSTable文件大小上限
	defaultSSTableSize = 5 * 1024 * 1024 // 5 MB
)
