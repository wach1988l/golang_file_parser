package main

import (
	"Verve-Test_project/parser"
	"Verve-Test_project/storage"
	"flag"
	"fmt"
	"strconv"
)

func main() {
	//var filePath = "/Users/vachaganlalayan/Downloads/promotions.csv"
	filePath := flag.String("path", "", "Path File")
	flag.Parse()

	redisClient := storage.NewRedisClient()
	oldVersion := redisClient.Get("file_version")
	versionInt, _ := strconv.Atoi(oldVersion)
	versionInt++
	isProcessed := parser.ReadAndSaveData(*filePath, strconv.Itoa(versionInt))
	if isProcessed {
		redisClient.DeleteAllByVersion(oldVersion)
		redisClient.Save("file_version", strconv.Itoa(versionInt))
		fmt.Println(versionInt)
	}

}
