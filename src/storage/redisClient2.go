package storage

import (
	"Verve-Test_project/model"
	"fmt"
	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	Client *redis.Client
}

func NewRedisClient() *RedisClient {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return &RedisClient{
		Client: client,
	}
}

func (r *RedisClient) Close() error {
	return r.Client.Close()
}

func (r *RedisClient) Get(key string) string {
	val, _ := r.Client.Get(r.Client.Context(), key).Result()
	return val
}

func (r *RedisClient) Save(key string, val string) {
	r.Client.Set(r.Client.Context(), key, val, 0)
}

func (r *RedisClient) DeleteAllByVersion(version string) {
	keys, err := r.Client.Keys(r.Client.Context(), version+"_*").Result()
	if err != nil {
		fmt.Println("Error retrieving keys:", err)
		return
	}

	pipeline := r.Client.Pipeline()
	for _, key := range keys {
		pipeline.Del(r.Client.Context(), key)
	}

	_, err = pipeline.Exec(r.Client.Context())
	if err != nil {
		fmt.Println("Error executing pipeline:", err)
		return
	}

	fmt.Printf("Removed Successfully %d keys\n", len(keys))
}

func (r *RedisClient) SaveBatch2(data []model.CSVData) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pipeline := redisClient.Pipeline()

	for _, csvData := range data {
		binaryData, _ := csvData.MarshalToBinary()
		pipeline.Set(redisClient.Context(), csvData.NumId, binaryData, 0)
	}

	_, err := pipeline.Exec(redisClient.Context())
	if err != nil {
		fmt.Println("Error executing command:", err)
		return
	}

	fmt.Println("Batch save completed successfully.")
}
