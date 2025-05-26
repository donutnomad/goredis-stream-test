package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// CleanupDemoHandler 清理演示处理器
type CleanupDemoHandler struct {
	name string
}

func (h *CleanupDemoHandler) GetMessageType() string {
	return "demo"
}

func (h *CleanupDemoHandler) Handle(ctx context.Context, msg *queue.Message) error {
	index := "未知"
	if idx, ok := msg.Data["index"]; ok {
		index = fmt.Sprintf("%d", int(idx.(float64)))
	}

	fmt.Printf("[%s] 🔄 处理消息: %s (索引: %s)\n", h.name, msg.ID, index)

	// 模拟处理时间
	time.Sleep(time.Millisecond * 300)

	fmt.Printf("[%s] ✅ 消息处理完成: %s\n", h.name, msg.ID)
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

	streamName := "cleanup-demo"

	fmt.Println("🎯 演示消息清理功能")
	fmt.Println()

	// 第一步：创建多个消费者组
	fmt.Println("📝 步骤1: 创建多个消费者组")

	consumer1 := queue.NewMessageQueue(rdb, streamName, "group1", "consumer1")
	consumer1.RegisterHandler(&CleanupDemoHandler{name: "消费者组1"})

	consumer2 := queue.NewMessageQueue(rdb, streamName, "group2", "consumer2")
	consumer2.RegisterHandler(&CleanupDemoHandler{name: "消费者组2"})

	// 启动消费者
	err = consumer1.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者1失败: %v", err)
	}

	err = consumer2.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者2失败: %v", err)
	}

	fmt.Println("✅ 两个消费者组已启动")
	fmt.Println()

	// 第二步：发布大量消息
	fmt.Println("📝 步骤2: 发布大量消息")

	producer := queue.NewProducer(rdb, streamName)
	consumer := queue.NewMessageQueue(rdb, streamName, "group1", "producer")

	for i := 0; i < 20; i++ {
		messageID, err := producer.PublishMessage(ctx, "demo", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("演示消息 %d", i),
		}, map[string]string{
			"batch": "cleanup-demo",
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("📤 发布消息 %d: %s\n", i, messageID)
		}
		time.Sleep(time.Millisecond * 200)
	}

	fmt.Println()

	// 第三步：等待消息被处理
	fmt.Println("📝 步骤3: 等待消息被两个消费者组处理")
	time.Sleep(time.Second * 10)

	// 第四步：检查Stream状态
	fmt.Println("📝 步骤4: 检查Stream状态")
	showStreamInfo(ctx, consumer)

	// 第五步：演示手动清理
	fmt.Println("📝 步骤5: 演示手动清理")

	// 设置清理策略
	cleanupPolicy := &queue.CleanupPolicy{
		EnableAutoCleanup: false,           // 手动清理
		MaxStreamLength:   10,              // 低阈值便于演示
		MinRetentionTime:  time.Second * 2, // 短保留时间
		BatchSize:         5,
	}
	consumer.SetCleanupPolicy(cleanupPolicy)

	// 等待一会儿确保消息足够老
	fmt.Println("⏳ 等待消息变老...")
	time.Sleep(time.Second * 3)

	cleaned, err := consumer.CleanupMessages(ctx)
	if err != nil {
		log.Printf("清理消息失败: %v", err)
	} else {
		fmt.Printf("✅ 手动清理完成，清理了 %d 条消息\n", cleaned)
	}

	fmt.Println()
	fmt.Println("📝 步骤6: 检查清理后的Stream状态")
	showStreamInfo(ctx, consumer)

	// 第六步：演示自动清理
	fmt.Println("📝 步骤7: 演示自动清理")

	// 创建启用自动清理的新消费者
	autoConsumer := queue.NewMessageQueue(rdb, streamName+"-auto", "auto-group", "auto-consumer")
	autoConsumer.RegisterHandler(&CleanupDemoHandler{name: "自动清理消费者"})

	// 设置激进的自动清理策略
	autoCleanupPolicy := &queue.CleanupPolicy{
		EnableAutoCleanup: true,
		CleanupInterval:   time.Second * 3, // 短间隔便于演示
		MaxStreamLength:   5,               // 低阈值
		MinRetentionTime:  time.Second * 1, // 短保留时间
		BatchSize:         3,
	}
	autoConsumer.SetCleanupPolicy(autoCleanupPolicy)

	err = autoConsumer.Start(ctx)
	if err != nil {
		log.Fatalf("启动自动清理消费者失败: %v", err)
	}

	fmt.Println("✅ 启用自动清理的消费者已启动")

	// 发布更多消息观察自动清理
	autoProducer := queue.NewProducer(rdb, streamName+"-auto")

	fmt.Println("📤 发布消息并观察自动清理...")
	for i := 0; i < 15; i++ {
		messageID, err := autoProducer.PublishMessage(ctx, "demo", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("自动清理消息 %d", i),
		}, map[string]string{
			"batch": "auto-cleanup",
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			fmt.Printf("📤 发布消息 %d: %s\n", i, messageID)
		}

		// 每发布几条消息检查一次Stream状态
		if (i+1)%5 == 0 {
			info, err := autoProducer.GetTopicInfo(ctx)
			if err != nil {
				log.Printf("获取topic信息失败: %v", err)
			} else {
				fmt.Printf("📊 当前Stream长度: %d\n", info.Length)
			}
		}

		time.Sleep(time.Millisecond * 800)
	}

	// 等待自动清理运行
	fmt.Println("⏳ 等待自动清理运行...")
	time.Sleep(time.Second * 8)

	// 检查最终状态
	fmt.Println("📝 步骤8: 检查自动清理后的状态")
	info, err := autoProducer.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取topic信息失败: %v", err)
	} else {
		fmt.Printf("📊 最终Stream长度: %d\n", info.Length)
		if info.Length <= autoCleanupPolicy.MaxStreamLength {
			fmt.Println("✅ 自动清理成功，Stream长度在限制范围内")
		} else {
			fmt.Printf("⚠️ Stream长度 %d 仍超过限制 %d\n", info.Length, autoCleanupPolicy.MaxStreamLength)
		}
	}

	// 清理
	consumer1.Stop()
	consumer2.Stop()
	autoConsumer.Stop()

	fmt.Println()
	fmt.Println("✅ 消息清理演示完成！")
	fmt.Println()
	fmt.Println("📋 总结:")
	fmt.Println("  - 手动清理：可以按需清理已被所有消费者组处理的消息")
	fmt.Println("  - 自动清理：定期检查并清理，保持Stream在合理大小")
	fmt.Println("  - 安全机制：只清理被所有消费者组确认且足够老的消息")
	fmt.Println("  - 批量处理：避免一次性删除太多消息影响性能")
}

func showStreamInfo(ctx context.Context, mq *queue.MessageQueue) {
	info, err := mq.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取topic信息失败: %v", err)
		return
	}

	fmt.Printf("📊 Stream: %s\n", info.StreamName)
	fmt.Printf("📊 消息总数: %d\n", info.Length)
	fmt.Printf("📊 消费者组数量: %d\n", len(info.Groups))

	for _, group := range info.Groups {
		fmt.Printf("📊   组 %s: pending=%d, lastDelivered=%s\n",
			group.Name, group.Pending, group.LastDeliveredID)
		for _, consumer := range group.Consumers {
			fmt.Printf("📊     消费者 %s: pending=%d, idle=%v\n",
				consumer.Name, consumer.Pending, consumer.Idle)
		}
	}
	fmt.Println()
}
