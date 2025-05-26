package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		action     = flag.String("action", "info", "操作类型: info, terminate")
		streamName = flag.String("stream", "", "Stream名称")
		groupName  = flag.String("group", "default-group", "消费者组名称")
		redisAddr  = flag.String("redis", "localhost:6379", "Redis地址")
	)
	flag.Parse()

	if *streamName == "" {
		fmt.Println("使用方法:")
		fmt.Println("  go run cmd/topic_manager.go -action=info -stream=my-stream")
		fmt.Println("  go run cmd/topic_manager.go -action=terminate -stream=my-stream")
		fmt.Println("")
		fmt.Println("参数:")
		fmt.Println("  -action    操作类型 (info|terminate)")
		fmt.Println("  -stream    Stream名称 (必需)")
		fmt.Println("  -group     消费者组名称 (默认: default-group)")
		fmt.Println("  -redis     Redis地址 (默认: localhost:6379)")
		os.Exit(1)
	}

	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr: *redisAddr,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("连接Redis失败: %v", err)
	}

	// 创建消息队列管理器
	mq := queue.NewMessageQueue(rdb, *streamName, *groupName, "manager")

	switch *action {
	case "info":
		showTopicInfo(ctx, mq)
	case "terminate":
		terminateTopic(ctx, mq)
	default:
		log.Fatalf("未知操作: %s", *action)
	}
}

func showTopicInfo(ctx context.Context, mq *queue.MessageQueue) {
	fmt.Println("📊 获取Topic信息...")

	info, err := mq.GetTopicInfo(ctx)
	if err != nil {
		log.Fatalf("获取topic信息失败: %v", err)
	}

	if !info.Exists {
		fmt.Printf("❌ Stream '%s' 不存在\n", info.StreamName)
		return
	}

	fmt.Printf("✅ Stream: %s\n", info.StreamName)
	fmt.Printf("   消息数量: %d\n", info.Length)
	fmt.Printf("   第一条消息ID: %s\n", info.FirstEntryID)
	fmt.Printf("   最后一条消息ID: %s\n", info.LastEntryID)
	fmt.Printf("   消费者组数量: %d\n", len(info.Groups))

	for i, group := range info.Groups {
		fmt.Printf("   \n   消费者组 %d:\n", i+1)
		fmt.Printf("     名称: %s\n", group.Name)
		fmt.Printf("     Pending消息: %d\n", group.Pending)
		fmt.Printf("     最后交付ID: %s\n", group.LastDeliveredID)
		fmt.Printf("     消费者数量: %d\n", len(group.Consumers))

		for j, consumer := range group.Consumers {
			fmt.Printf("       消费者 %d: %s (Pending: %d, Idle: %v)\n",
				j+1, consumer.Name, consumer.Pending, consumer.Idle)
		}
	}

	// 输出JSON格式
	fmt.Println("\n📋 详细信息 (JSON):")
	jsonData, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		log.Printf("JSON序列化失败: %v", err)
	} else {
		fmt.Println(string(jsonData))
	}
}

func terminateTopic(ctx context.Context, mq *queue.MessageQueue) {
	fmt.Printf("⚠️  警告: 即将终止Topic '%s'\n", mq.GetStreamName())
	fmt.Println("   这将删除:")
	fmt.Println("   - 所有未处理的消息")
	fmt.Println("   - 所有消费者组")
	fmt.Println("   - 所有Pending消息")
	fmt.Println("")

	// 先显示当前状态
	info, err := mq.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取topic信息失败: %v", err)
	} else if info.Exists {
		fmt.Printf("   当前状态: %d 条消息, %d 个消费者组\n", info.Length, len(info.Groups))
	} else {
		fmt.Println("   Topic不存在，无需终止")
		return
	}

	fmt.Print("\n确认终止? (输入 'yes' 确认): ")
	var confirm string
	fmt.Scanln(&confirm)

	if confirm != "yes" {
		fmt.Println("❌ 操作已取消")
		return
	}

	fmt.Println("🛑 正在终止Topic...")
	err = mq.TerminateTopic(ctx)
	if err != nil {
		log.Fatalf("终止Topic失败: %v", err)
	}

	fmt.Println("✅ Topic已成功终止")

	// 验证终止结果
	info, err = mq.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("验证失败: %v", err)
	} else if !info.Exists {
		fmt.Println("✅ 验证: Topic已完全清空")
	} else {
		fmt.Printf("⚠️  警告: Topic仍然存在 (%d 条消息)\n", info.Length)
	}
}
