package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// EmailBatchHandler 邮件批量处理器
type EmailBatchHandler struct {
	name string
}

func (h *EmailBatchHandler) GetMessageType() string {
	return "email"
}

func (h *EmailBatchHandler) GetBatchSize() int {
	return 5 // 每批处理5封邮件
}

func (h *EmailBatchHandler) HandleBatch(ctx context.Context, messages []*queue.Message) error {
	fmt.Printf("📧 [%s] 开始批量发送 %d 封邮件\n", h.name, len(messages))

	// 模拟批量邮件发送
	var recipients []string
	for i, msg := range messages {
		if to, ok := msg.Data["to"].(string); ok {
			recipients = append(recipients, to)
			fmt.Printf("📧 [%s] 准备邮件 %d/%d: 发送给 %s\n",
				h.name, i+1, len(messages), to)
		}
	}

	// 模拟批量发送API调用
	fmt.Printf("📧 [%s] 调用批量邮件API，发送给: %v\n", h.name, recipients)
	time.Sleep(time.Millisecond * 500) // 模拟API调用时间

	fmt.Printf("✅ [%s] 批量邮件发送完成，共发送 %d 封\n", h.name, len(messages))
	return nil
}

// OrderHandler 订单单条处理器（不支持批量）
type OrderHandler struct {
	name string
}

func (h *OrderHandler) GetMessageType() string {
	return "order"
}

func (h *OrderHandler) Handle(ctx context.Context, msg *queue.Message) error {
	orderID := "未知"
	if id, ok := msg.Data["order_id"].(string); ok {
		orderID = id
	}

	fmt.Printf("📦 [%s] 处理订单: %s\n", h.name, orderID)

	// 模拟订单处理
	time.Sleep(time.Millisecond * 200)

	fmt.Printf("✅ [%s] 订单处理完成: %s\n", h.name, orderID)
	return nil
}

// SMSBatchHandler 短信批量处理器
type SMSBatchHandler struct {
	name string
}

func (h *SMSBatchHandler) GetMessageType() string {
	return "sms"
}

func (h *SMSBatchHandler) GetBatchSize() int {
	return 10 // 每批处理10条短信
}

func (h *SMSBatchHandler) HandleBatch(ctx context.Context, messages []*queue.Message) error {
	fmt.Printf("📱 [%s] 开始批量发送 %d 条短信\n", h.name, len(messages))

	// 模拟批量短信发送
	for i, msg := range messages {
		phone := "未知"
		if p, ok := msg.Data["phone"].(string); ok {
			phone = p
		}
		fmt.Printf("📱 [%s] 准备短信 %d/%d: 发送给 %s\n",
			h.name, i+1, len(messages), phone)
	}

	// 模拟批量发送
	time.Sleep(time.Millisecond * 300)

	fmt.Printf("✅ [%s] 批量短信发送完成，共发送 %d 条\n", h.name, len(messages))
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

	streamName := "batch-demo"

	fmt.Println("🎯 演示批量处理功能")
	fmt.Println()

	// 创建消费者
	consumer := queue.NewMessageQueue(rdb, streamName, "demo-group", "batch-consumer")

	// 配置批量处理
	batchConfig := &queue.BatchConfig{
		EnableBatch:  true,
		BatchSize:    8,                      // 默认批量大小
		BatchTimeout: time.Millisecond * 800, // 800ms超时
		MaxWaitTime:  time.Second * 3,        // 最大等待3秒
	}
	consumer.SetBatchConfig(batchConfig)

	// 注册处理器
	emailHandler := &EmailBatchHandler{name: "邮件服务"}
	smsHandler := &SMSBatchHandler{name: "短信服务"}
	orderHandler := &OrderHandler{name: "订单服务"}

	consumer.RegisterBatchHandler(emailHandler)
	consumer.RegisterBatchHandler(smsHandler)
	consumer.RegisterHandler(orderHandler) // 订单使用单条处理

	// 启动消费者
	err = consumer.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	fmt.Println("✅ 批量处理消费者已启动")
	fmt.Println()

	// 创建生产者
	producer := queue.NewMessageQueue(rdb, streamName, "demo-group", "batch-producer")

	// 演示1：批量邮件处理
	fmt.Println("📝 演示1: 批量邮件处理")
	fmt.Println("发布12封邮件（将分为3批：5+5+2）")

	for i := 0; i < 12; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      fmt.Sprintf("user%d@example.com", i),
			"subject": fmt.Sprintf("邮件主题 %d", i),
			"body":    fmt.Sprintf("这是第 %d 封邮件的内容", i),
		}, map[string]string{
			"priority": "normal",
		})
		if err != nil {
			log.Printf("发布邮件失败: %v", err)
		} else {
			fmt.Printf("📤 发布邮件 %d: %s\n", i, messageID)
		}
		time.Sleep(time.Millisecond * 200)
	}

	fmt.Println()
	time.Sleep(time.Second * 3)

	// 演示2：批量短信处理
	fmt.Println("📝 演示2: 批量短信处理")
	fmt.Println("发布15条短信（将分为2批：10+5）")

	for i := 0; i < 15; i++ {
		messageID, err := producer.PublishMessage(ctx, "sms", map[string]interface{}{
			"phone":   fmt.Sprintf("138%08d", i),
			"content": fmt.Sprintf("短信内容 %d", i),
		}, map[string]string{
			"type": "notification",
		})
		if err != nil {
			log.Printf("发布短信失败: %v", err)
		} else {
			fmt.Printf("📤 发布短信 %d: %s\n", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	fmt.Println()
	time.Sleep(time.Second * 2)

	// 演示3：单条订单处理
	fmt.Println("📝 演示3: 单条订单处理")
	fmt.Println("发布6个订单（单条处理）")

	for i := 0; i < 6; i++ {
		messageID, err := producer.PublishMessage(ctx, "order", map[string]interface{}{
			"order_id": fmt.Sprintf("ORDER-%06d", i+1),
			"amount":   float64(100 + i*10),
			"status":   "pending",
		}, map[string]string{
			"source": "web",
		})
		if err != nil {
			log.Printf("发布订单失败: %v", err)
		} else {
			fmt.Printf("📤 发布订单 %d: %s\n", i, messageID)
		}
		time.Sleep(time.Millisecond * 300)
	}

	fmt.Println()
	time.Sleep(time.Second * 3)

	// 演示4：混合消息处理
	fmt.Println("📝 演示4: 混合消息处理")
	fmt.Println("同时发布邮件、短信和订单")

	for i := 0; i < 5; i++ {
		// 发布邮件
		emailID, _ := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      fmt.Sprintf("mixed%d@example.com", i),
			"subject": fmt.Sprintf("混合测试邮件 %d", i),
			"body":    "混合测试内容",
		}, map[string]string{
			"batch": "mixed",
		})
		fmt.Printf("📤 发布混合邮件 %d: %s\n", i, emailID)

		// 发布短信
		smsID, _ := producer.PublishMessage(ctx, "sms", map[string]interface{}{
			"phone":   fmt.Sprintf("139%08d", i),
			"content": fmt.Sprintf("混合测试短信 %d", i),
		}, map[string]string{
			"batch": "mixed",
		})
		fmt.Printf("📤 发布混合短信 %d: %s\n", i, smsID)

		// 发布订单
		orderID, _ := producer.PublishMessage(ctx, "order", map[string]interface{}{
			"order_id": fmt.Sprintf("MIX-%06d", i+1),
			"amount":   float64(200 + i*20),
			"status":   "pending",
		}, map[string]string{
			"batch": "mixed",
		})
		fmt.Printf("📤 发布混合订单 %d: %s\n", i, orderID)

		time.Sleep(time.Millisecond * 200)
	}

	fmt.Println()
	time.Sleep(time.Second * 4)

	// 演示5：超时处理
	fmt.Println("📝 演示5: 超时处理")
	fmt.Println("发布少量消息测试超时机制")

	for i := 0; i < 3; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      fmt.Sprintf("timeout%d@example.com", i),
			"subject": "超时测试邮件",
			"body":    "测试超时处理机制",
		}, map[string]string{
			"test": "timeout",
		})
		if err != nil {
			log.Printf("发布超时测试邮件失败: %v", err)
		} else {
			fmt.Printf("📤 发布超时测试邮件 %d: %s\n", i, messageID)
		}
		time.Sleep(time.Millisecond * 300)
	}

	fmt.Println("⏳ 等待超时处理...")
	time.Sleep(time.Second * 5)

	// 检查最终状态
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		log.Printf("获取topic信息失败: %v", err)
	} else {
		fmt.Printf("📊 最终Stream长度: %d\n", info.Length)
		for _, group := range info.Groups {
			fmt.Printf("📊 组 %s: pending=%d\n", group.Name, group.Pending)
		}
	}

	consumer.Stop()

	fmt.Println()
	fmt.Println("✅ 批量处理演示完成！")
	fmt.Println()
	fmt.Println("📋 总结:")
	fmt.Println("  - 邮件：使用批量处理，每批5封")
	fmt.Println("  - 短信：使用批量处理，每批10条")
	fmt.Println("  - 订单：使用单条处理")
	fmt.Println("  - 自动分批：超过批量大小时自动分批")
	fmt.Println("  - 超时处理：不足一批时等待超时后处理")
	fmt.Println("  - 混合处理：同时支持批量和单条处理器")
}
