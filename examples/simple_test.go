package main

import (
	"context"
	"log"
	"time"

	"goStream/handlers"
	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// 测试Redis连接
	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Printf("警告：无法连接到Redis: %v", err)
		log.Println("请确保Redis服务器正在运行在 localhost:6379")
		log.Println("你可以使用以下命令启动Redis:")
		log.Println("  docker run -d -p 6379:6379 redis:latest")
		log.Println("或者:")
		log.Println("  redis-server")
		return
	}
	log.Println("✅ Redis连接成功")

	// 创建消息队列
	producer := queue.NewProducer(rdb, "test-stream")
	mq := queue.NewMessageQueue(rdb, "test-stream", "test-group", "test-consumer")

	// 注册处理器
	mq.RegisterHandler(handlers.NewEmailHandler())
	mq.RegisterHandler(handlers.NewOrderHandler())

	// 启动消息队列
	log.Println("🚀 启动消息队列...")
	err = mq.Start(ctx)
	if err != nil {
		log.Fatalf("启动消息队列失败: %v", err)
	}

	// 等待一会儿让消费者准备好
	time.Sleep(time.Second * 2)

	// 发布测试消息
	log.Println("📤 发布测试消息...")

	// 发布邮件消息
	emailID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
		"to":      "test@example.com",
		"subject": "测试邮件",
		"body":    "这是一个测试邮件",
	}, map[string]string{
		"priority": "high",
	})
	if err != nil {
		log.Printf("发布邮件消息失败: %v", err)
	} else {
		log.Printf("✅ 邮件消息已发布: %s", emailID)
	}

	// 发布订单消息
	orderID, err := producer.PublishMessage(ctx, "order", map[string]interface{}{
		"order_id": "TEST-ORDER-001",
		"user_id":  "USER-123",
		"amount":   99.99,
	}, map[string]string{
		"priority": "medium",
	})
	if err != nil {
		log.Printf("发布订单消息失败: %v", err)
	} else {
		log.Printf("✅ 订单消息已发布: %s", orderID)
	}

	// 等待消息处理
	log.Println("⏳ 等待消息处理...")
	time.Sleep(time.Second * 5)

	// 演示停止消息处理
	log.Println("🛑 演示停止消息处理功能...")

	// // 发布一个将被停止的消息
	// stoppedMsgID, err := mq.PublishMessage(ctx, "email", map[string]interface{}{
	// 	"to":      "stopped@example.com",
	// 	"subject": "将被停止的消息",
	// 	"body":    "这个消息将被停止处理",
	// }, map[string]string{
	// 	"test": "stop",
	// })
	// if err != nil {
	// 	log.Printf("发布停止测试消息失败: %v", err)
	// } else {
	// 	log.Printf("📤 发布了将被停止的消息: %s", stoppedMsgID)

	// 	// 立即停止这个消息的处理
	// 	time.Sleep(time.Second)
	// 	mq.StopMessageProcessing(stoppedMsgID)

	// 	// 等待一会儿，然后恢复处理
	// 	time.Sleep(time.Second * 3)
	// 	log.Println("🔄 恢复消息处理...")
	// 	mq.ResumeMessageProcessing(stoppedMsgID)
	// }

	// 等待所有消息处理完成
	time.Sleep(time.Second * 10)

	// 停止消息队列
	log.Println("🛑 停止消息队列...")
	mq.Stop()

	log.Println("✅ 测试完成！")

	// 显示一些有用的Redis命令
	log.Println("\n📊 你可以使用以下Redis命令查看Stream信息:")
	log.Println("  redis-cli XINFO STREAM test-stream")
	log.Println("  redis-cli XINFO GROUPS test-stream")
	log.Println("  redis-cli XPENDING test-stream test-group")
}
