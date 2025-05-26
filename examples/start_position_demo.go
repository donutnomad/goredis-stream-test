package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// DemoHandler 演示处理器
type DemoHandler struct {
	name string
}

func (h *DemoHandler) GetMessageType() string {
	return "demo"
}

func (h *DemoHandler) Handle(ctx context.Context, msg *queue.Message) error {
	fmt.Printf("[%s] 处理消息: %s, 内容: %v\n", h.name, msg.ID, msg.Data)
	return nil
}

func main() {
	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx := context.Background()

	// 测试连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("无法连接到Redis: %v", err)
	}

	streamName := "demo-stream"

	fmt.Println("🎯 演示不同的消费者开始位置")
	fmt.Println()

	// 第一步：发布一些历史消息
	fmt.Println("📝 步骤1: 发布历史消息")
	producer := queue.NewMessageQueue(rdb, streamName, "demo-group", "producer")

	for i := 0; i < 5; i++ {
		messageID, err := producer.PublishMessage(ctx, "demo", map[string]interface{}{
			"message": fmt.Sprintf("历史消息 %d", i),
			"index":   i,
		}, map[string]string{
			"type": "historical",
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("📤 发布历史消息 %d: %s\n", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	fmt.Println()

	// 第二步：演示从最新位置开始的消费者
	fmt.Println("🔄 步骤2: 创建从最新位置开始的消费者")
	consumerLatest := queue.NewMessageQueue(rdb, streamName, "latest-group", "consumer-latest")
	consumerLatest.RegisterHandler(&DemoHandler{name: "最新位置消费者"})

	err = consumerLatest.Start(ctx)
	if err != nil {
		log.Fatalf("启动最新位置消费者失败: %v", err)
	}

	time.Sleep(time.Second * 2)

	// 发布新消息
	fmt.Println("📤 发布新消息给最新位置消费者...")
	for i := 0; i < 2; i++ {
		messageID, err := producer.PublishMessage(ctx, "demo", map[string]interface{}{
			"message": fmt.Sprintf("新消息 %d", i),
			"index":   i + 100,
		}, map[string]string{
			"type": "new",
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("📤 发布新消息: %s\n", messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	time.Sleep(time.Second * 3)
	consumerLatest.Stop()

	fmt.Println()

	// 第三步：演示从最早位置开始的消费者
	fmt.Println("🔄 步骤3: 创建从最早位置开始的消费者")
	consumerEarliest := queue.NewMessageQueueWithStartPosition(rdb, streamName, "earliest-group", "consumer-earliest", queue.StartFromEarliest)
	consumerEarliest.RegisterHandler(&DemoHandler{name: "最早位置消费者"})

	err = consumerEarliest.Start(ctx)
	if err != nil {
		log.Fatalf("启动最早位置消费者失败: %v", err)
	}

	time.Sleep(time.Second * 5)
	consumerEarliest.Stop()

	fmt.Println()

	// 第四步：演示从指定ID开始的消费者
	fmt.Println("🔄 步骤4: 创建从指定ID开始的消费者")

	// 先获取当前stream的信息来选择一个中间的消息ID
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取topic信息失败: %v", err)
		return
	}

	if info.Exists && info.Length > 2 {
		// 使用一个简单的策略：从第一个消息ID开始，但跳过前面几条
		fmt.Printf("📊 当前Stream有 %d 条消息\n", info.Length)

		// 发布几条新消息用于演示
		var specificID string
		for i := 0; i < 3; i++ {
			messageID, err := producer.PublishMessage(ctx, "demo", map[string]interface{}{
				"message": fmt.Sprintf("指定位置测试消息 %d", i),
				"index":   i + 200,
			}, map[string]string{
				"type": "specific-test",
			})
			if err != nil {
				log.Printf("发布消息失败: %v", err)
			} else {
				fmt.Printf("📤 发布测试消息: %s\n", messageID)
				if i == 1 { // 从第二条消息开始
					specificID = messageID
				}
			}
			time.Sleep(time.Millisecond * 100)
		}

		if specificID != "" {
			fmt.Printf("🎯 将从消息ID %s 开始消费\n", specificID)

			consumerSpecific := queue.NewMessageQueueFromSpecificID(rdb, streamName, "specific-group", "consumer-specific", specificID)
			consumerSpecific.RegisterHandler(&DemoHandler{name: "指定位置消费者"})

			err = consumerSpecific.Start(ctx)
			if err != nil {
				log.Fatalf("启动指定位置消费者失败: %v", err)
			}

			time.Sleep(time.Second * 3)
			consumerSpecific.Stop()
		}
	}

	fmt.Println()
	fmt.Println("✅ 演示完成！")
	fmt.Println()
	fmt.Println("📋 总结:")
	fmt.Println("  - 最新位置消费者：只处理启动后的新消息")
	fmt.Println("  - 最早位置消费者：处理所有历史消息和新消息")
	fmt.Println("  - 指定位置消费者：从指定消息ID开始处理")
}
