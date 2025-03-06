package lsmtree

import (
	"os"
)

func GetFileSize(filePath string) (int64, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func GetDatabaseSourcePath() string {
	home := os.Getenv("HOME") // 在类Unix系统中
	if home == "" {
		home = os.Getenv("USERPROFILE") // 在Windows系统中
	}
	return home + databaseSourcePath
}
