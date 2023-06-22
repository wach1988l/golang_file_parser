package main

import (
	"Verve-Test_project/parser"
	"Verve-Test_project/storage"
	"fmt"
	"strconv"
)

func main() {
	var filePath = "/Users/vachaganlalayan/Downloads/promotions.csv"
	redisClient := storage.NewRedisClient()
	oldVersion := redisClient.Get("file_version")
	versionInt, _ := strconv.Atoi(oldVersion)
	versionInt++
	isProcessed := parser.ReadAndSaveData(filePath, strconv.Itoa(versionInt))
	if isProcessed {
		redisClient.DeleteAllByVersion(oldVersion)
		redisClient.Save("file_version", strconv.Itoa(versionInt))
		fmt.Println(versionInt)
	}

}
