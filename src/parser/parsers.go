package parser

import (
	"Verve-Test_project/model"
	"Verve-Test_project/storage"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
)

func ReadAndSaveData(filePath string, version string) bool {
	isProcessed := true
	redisClient := storage.NewRedisClient()
	file, err := os.Open(filePath)
	if err != nil {
		isProcessed = false
		log.Fatal(err)

	}
	defer file.Close()
	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		isProcessed = false
		log.Fatal(err)

	}

	batchSize := 1000
	batch := make([][]string, 0, batchSize)
	id := 0
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatal(err)
		}
		batch = append(batch, record)
		if len(batch) == batchSize {
			id = processBatch(batch, id, redisClient, version)
			batch = make([][]string, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		processBatch(batch, id, redisClient, version)
	}

	fmt.Println("Parsing is Finished")
	return isProcessed
}

func processBatch(batch [][]string, id int, client *storage.RedisClient, version string) int {
	datas := make([]model.CSVData, len(batch))
	for _, record := range batch {
		id++
		price, _ := strconv.ParseFloat(record[1], 64)
		cd := model.CSVData{
			NumId:      version + "_" + strconv.Itoa(id),
			Id:         record[0],
			Price:      price,
			ExpireDate: record[2],
		}
		datas = append(datas, cd)
	}
	client.SaveBatch2(datas)
	return id
}
