package main

import (
	"context"
	"encoding/json"
	"fmt"
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
		log.Println("请先运行: make redis-up")
		return
	}
	log.Println("✅ Redis连接成功")

	// 演示topic终止功能
	demonstrateTopicTermination(ctx, rdb)
}

func demonstrateTopicTermination(ctx context.Context, rdb *redis.Client) {
	streamName := "terminate-demo"
	groupName := "demo-group"

	log.Println("🎯 演示Topic终止功能")

	// 第一步：创建多个消费者和大量消息
	log.Println("📝 步骤1: 创建消费者并发布大量消息")

	// 创建生产者
	producer := queue.NewMessageQueue(rdb, streamName, groupName, "producer")

	// 发布大量消息
	log.Println("📤 发布大量测试消息...")
	messageIDs := make([]string, 0)

	for i := 0; i < 50; i++ {
		var msgType string
		var data map[string]interface{}

		if i%2 == 0 {
			msgType = "email"
			data = map[string]interface{}{
				"to":      fmt.Sprintf("user%d@example.com", i),
				"subject": fmt.Sprintf("测试邮件 %d", i),
				"body":    fmt.Sprintf("这是第 %d 封测试邮件", i),
			}
		} else {
			msgType = "order"
			data = map[string]interface{}{
				"order_id": fmt.Sprintf("ORDER-%03d", i),
				"user_id":  fmt.Sprintf("USER-%d", i),
				"amount":   float64(i) * 10.5,
			}
		}

		messageID, err := producer.PublishMessage(ctx, msgType, data, map[string]string{
			"batch":    "terminate-test",
			"priority": "normal",
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			messageIDs = append(messageIDs, messageID)
			if i%10 == 0 {
				log.Printf("📤 已发布 %d 条消息", i+1)
			}
		}
	}

	log.Printf("✅ 总共发布了 %d 条消息", len(messageIDs))

	// 第二步：启动多个消费者开始处理
	log.Println("🔄 步骤2: 启动多个消费者开始处理消息")

	consumer1 := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-1")
	consumer1.RegisterHandler(handlers.NewEmailHandler())
	consumer1.RegisterHandler(handlers.NewOrderHandler())

	consumer2 := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-2")
	consumer2.RegisterHandler(handlers.NewEmailHandler())
	consumer2.RegisterHandler(handlers.NewOrderHandler())

	// 启动消费者
	err := consumer1.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者1失败: %v", err)
	}

	err = consumer2.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者2失败: %v", err)
	}

	log.Println("✅ 消费者已启动，开始处理消息...")

	// 让消费者处理一些消息
	time.Sleep(time.Second * 10)

	// 第三步：查看topic信息
	log.Println("📊 步骤3: 查看topic当前状态")

	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取topic信息失败: %v", err)
	} else {
		printTopicInfo(info)
	}

	// 第四步：终止topic
	log.Println("🛑 步骤4: 终止整个topic")
	log.Println("⚠️  警告：即将删除所有未处理的消息和消费者组！")

	// 等待几秒钟让用户看到警告
	time.Sleep(time.Second * 3)

	// 使用任意一个消息队列实例来终止topic
	err = producer.TerminateTopic(ctx)
	if err != nil {
		log.Printf("终止topic失败: %v", err)
		return
	}

	// 第五步：验证topic已被清空
	log.Println("✅ 步骤5: 验证topic已被完全清空")

	time.Sleep(time.Second * 2)

	// 创建新的实例来检查
	checker := queue.NewMessageQueue(rdb, streamName, groupName, "checker")
	info, err = checker.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取topic信息失败: %v", err)
	} else {
		log.Println("📊 终止后的topic状态:")
		printTopicInfo(info)
	}

	// 第六步：演示可以重新创建topic
	log.Println("🔄 步骤6: 演示可以重新创建topic")

	newProducer := queue.NewMessageQueue(rdb, streamName, groupName, "new-producer")
	messageID, err := newProducer.PublishMessage(ctx, "email", map[string]interface{}{
		"to":      "test@example.com",
		"subject": "重新开始",
		"body":    "topic已重新创建",
	}, map[string]string{
		"status": "restarted",
	})

	if err != nil {
		log.Printf("重新发布消息失败: %v", err)
	} else {
		log.Printf("✅ 重新发布消息成功: %s", messageID)
	}

	// 最终状态检查
	info, err = newProducer.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取新topic信息失败: %v", err)
	} else {
		log.Println("📊 重新创建后的topic状态:")
		printTopicInfo(info)
	}

	log.Println("✅ Topic终止功能演示完成！")
}

func printTopicInfo(info *queue.TopicInfo) {
	if !info.Exists {
		log.Printf("  Stream: %s (不存在)", info.StreamName)
		return
	}

	log.Printf("  Stream: %s", info.StreamName)
	log.Printf("  消息数量: %d", info.Length)
	log.Printf("  第一条消息ID: %s", info.FirstEntryID)
	log.Printf("  最后一条消息ID: %s", info.LastEntryID)
	log.Printf("  消费者组数量: %d", len(info.Groups))

	for _, group := range info.Groups {
		log.Printf("    组: %s (Pending: %d)", group.Name, group.Pending)
		log.Printf("      最后交付ID: %s", group.LastDeliveredID)
		log.Printf("      消费者数量: %d", len(group.Consumers))

		for _, consumer := range group.Consumers {
			log.Printf("        消费者: %s (Pending: %d, Idle: %v)",
				consumer.Name, consumer.Pending, consumer.Idle)
		}
	}

	// 将信息转换为JSON格式便于查看
	jsonData, err := json.MarshalIndent(info, "  ", "  ")
	if err == nil {
		log.Printf("  详细信息 (JSON):\n%s", string(jsonData))
	}
}
