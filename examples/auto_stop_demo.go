package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
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
	log.Printf("[%s] 处理消息: %s, 数据: %v", h.name, msg.ID, msg.Data)

	// 模拟处理时间
	time.Sleep(time.Second * 2)

	log.Printf("[%s] 消息处理完成: %s", h.name, msg.ID)
	return nil
}

func main() {
	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	// 测试Redis连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("无法连接到Redis: %v", err)
	}
	log.Println("✅ Redis连接成功")

	streamName := "auto-stop-demo"
	groupName := "demo-group"

	// 清理可能存在的旧数据
	rdb.Del(ctx, streamName)
	time.Sleep(time.Millisecond * 100)

	log.Println("🎯 演示消费者自动停止功能")

	// 创建多个消费者
	numConsumers := 2
	consumers := make([]*queue.MessageQueue, numConsumers)
	var wg sync.WaitGroup

	// 启动消费者
	for i := 0; i < numConsumers; i++ {
		consumerName := fmt.Sprintf("consumer-%d", i)
		consumer := queue.NewMessageQueue(rdb, streamName, groupName, consumerName)
		consumer.RegisterHandler(&DemoHandler{name: consumerName})

		err := consumer.Start(ctx)
		if err != nil {
			log.Fatalf("启动消费者 %s 失败: %v", consumerName, err)
		}

		consumers[i] = consumer
		log.Printf("✅ 消费者 %s 已启动", consumerName)

		// 为每个消费者启动监控goroutine
		wg.Add(1)
		go func(c *queue.MessageQueue, name string) {
			defer wg.Done()
			log.Printf("👀 开始监控消费者 %s 的自动停止...", name)

			select {
			case <-c.Done():
				log.Printf("🛑 消费者 %s 已自动停止", name)
				if c.IsAutoStopped() {
					log.Printf("✅ 确认消费者 %s 是因为topic删除而自动停止", name)
				} else {
					log.Printf("ℹ️  消费者 %s 是手动停止", name)
				}
			}
		}(consumer, consumerName)

		time.Sleep(time.Millisecond * 500)
	}

	// 等待消费者启动
	time.Sleep(time.Second * 2)

	// 创建生产者并发布消息
	producer := queue.NewMessageQueue(rdb, streamName, groupName, "producer")

	log.Println("📝 发布消息...")
	for i := 0; i < 5; i++ {
		messageID, err := producer.PublishMessage(ctx, "demo", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("自动停止演示消息 %d", i),
			"time":    time.Now().Format(time.RFC3339),
		}, map[string]string{
			"demo": "auto-stop",
		})
		if err != nil {
			log.Printf("❌ 发布消息失败: %v", err)
		} else {
			log.Printf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 500)
	}

	// 让消费者处理一些消息
	log.Println("⏳ 等待消费者处理消息...")
	time.Sleep(time.Second * 5)

	// 演示手动停止一个消费者
	log.Println("🔧 手动停止第一个消费者...")
	consumers[0].Stop()

	// 等待一会儿
	time.Sleep(time.Second * 2)

	// 演示通过删除topic自动停止剩余消费者
	log.Println("🛑 删除topic以触发剩余消费者自动停止...")
	terminator := queue.NewMessageQueue(rdb, streamName, groupName, "terminator")
	err = terminator.TerminateTopic(ctx)
	if err != nil {
		log.Printf("❌ 删除topic失败: %v", err)
	} else {
		log.Println("✅ Topic已删除")
	}

	// 等待所有监控goroutine完成
	log.Println("⏳ 等待所有消费者停止...")
	wg.Wait()

	// 验证消费者状态
	log.Println("📊 验证消费者状态:")
	for i, consumer := range consumers {
		if consumer.IsAutoStopped() {
			log.Printf("✅ 消费者 %d: 自动停止", i)
		} else {
			log.Printf("ℹ️  消费者 %d: 手动停止", i)
		}
	}

	log.Println("🎉 自动停止功能演示完成！")

	// 演示使用场景说明
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📚 自动停止功能使用场景:")
	fmt.Println("1. 🔄 优雅关闭: 当topic被删除时，消费者自动停止，避免无效的重试")
	fmt.Println("2. 🛡️  错误处理: 检测到Stream或消费者组被删除时自动停止")
	fmt.Println("3. 📡 外部监控: 通过 <-consumer.Done() 监听消费者状态变化")
	fmt.Println("4. 🔍 状态检查: 通过 consumer.IsAutoStopped() 区分停止原因")
	fmt.Println("5. 🏗️  微服务架构: 在容器化环境中实现优雅的服务关闭")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("\n💡 使用示例:")
	fmt.Println("```go")
	fmt.Println("// 启动消费者")
	fmt.Println("consumer := queue.NewMessageQueue(rdb, \"my-stream\", \"my-group\", \"consumer-1\")")
	fmt.Println("consumer.Start(ctx)")
	fmt.Println("")
	fmt.Println("// 监听自动停止")
	fmt.Println("go func() {")
	fmt.Println("    <-consumer.Done()")
	fmt.Println("    if consumer.IsAutoStopped() {")
	fmt.Println("        log.Println(\"消费者因topic删除而自动停止\")")
	fmt.Println("    } else {")
	fmt.Println("        log.Println(\"消费者手动停止\")")
	fmt.Println("    }")
	fmt.Println("}()")
	fmt.Println("```")
}
