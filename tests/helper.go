package tests

import (
	"os"

	"github.com/redis/go-redis/v9"
)

// GetRedisClient 获取Redis客户端实例
func GetRedisClient() *redis.Client {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379" // 默认地址
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDB := 0 // 使用默认数据库

	return redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
}

// getRDB 为了兼容其他测试文件
func getRDB() *redis.Client {
	return GetRedisClient()
}
