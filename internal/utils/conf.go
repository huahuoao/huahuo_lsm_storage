package utils

import "os"

const (
	databaseSourcePath = "/lsm_huahuo/"
)

func GetDatabaseSourcePath() string {
	home := os.Getenv("HOME") // 在类Unix系统中
	if home == "" {
		home = os.Getenv("USERPROFILE") // 在Windows系统中
	}
	return home + databaseSourcePath
}
