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
		log.Println("请先运行: make redis-up")
		return
	}
	log.Println("✅ Redis连接成功")

	// 演示Pending消息处理
	demonstratePendingHandling(ctx, rdb)
}

func demonstratePendingHandling(ctx context.Context, rdb *redis.Client) {
	log.Println("🎯 演示Pending消息处理机制")

	// 第一步：启动一个消费者，让它处理一些消息但不完成
	log.Println("📝 步骤1: 启动第一个消费者，模拟处理中断")

	consumer1 := queue.NewMessageQueue(rdb, "pending-demo", "demo-group", "consumer-1")
	consumer1.RegisterHandler(handlers.NewEmailHandler())
	consumer1.RegisterHandler(handlers.NewOrderHandler())

	// 启动第一个消费者
	err := consumer1.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者1失败: %v", err)
	}

	// 发布一些消息
	log.Println("📤 发布测试消息...")
	for i := 0; i < 5; i++ {
		messageID, err := consumer1.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "测试邮件",
			"body":    "这是一个测试邮件",
		}, map[string]string{
			"batch": "pending-test",
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			log.Printf("📤 发布消息: %s", messageID)
		}
		time.Sleep(time.Millisecond * 500)
	}

	// 让消费者处理一会儿
	time.Sleep(time.Second * 3)

	// 突然停止第一个消费者（模拟崩溃）
	log.Println("💥 模拟消费者1崩溃...")
	consumer1.Stop()

	// 等待一会儿，让消息进入pending状态
	time.Sleep(time.Second * 2)

	// 第二步：启动第二个消费者，它应该处理pending消息
	log.Println("🔄 步骤2: 启动第二个消费者，处理pending消息")

	consumer2 := queue.NewMessageQueue(rdb, "pending-demo", "demo-group", "consumer-2")
	consumer2.RegisterHandler(handlers.NewEmailHandler())
	consumer2.RegisterHandler(handlers.NewOrderHandler())

	err = consumer2.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者2失败: %v", err)
	}

	// 让第二个消费者处理pending消息
	log.Println("⏳ 等待消费者2处理pending消息...")
	time.Sleep(time.Second * 10)

	// 第三步：启动第三个消费者，演示多消费者竞争
	log.Println("🏁 步骤3: 启动第三个消费者，演示竞争处理")

	consumer3 := queue.NewMessageQueue(rdb, "pending-demo", "demo-group", "consumer-3")
	consumer3.RegisterHandler(handlers.NewEmailHandler())
	consumer3.RegisterHandler(handlers.NewOrderHandler())

	err = consumer3.Start(ctx)
	if err != nil {
		log.Fatalf("启动消费者3失败: %v", err)
	}

	// 发布更多消息，让两个消费者竞争处理
	log.Println("📤 发布更多消息供竞争处理...")
	for i := 0; i < 10; i++ {
		messageID, err := consumer2.PublishMessage(ctx, "order", map[string]interface{}{
			"order_id": "ORDER-" + string(rune(i+1)),
			"user_id":  "USER-123",
			"amount":   99.99,
		}, map[string]string{
			"batch": "competition-test",
		})
		if err != nil {
			log.Printf("发布消息失败: %v", err)
		} else {
			log.Printf("📤 发布订单消息: %s", messageID)
		}
		time.Sleep(time.Millisecond * 200)
	}

	// 让消费者们竞争处理
	log.Println("⏳ 观察消费者竞争处理...")
	time.Sleep(time.Second * 15)

	// 演示消息停止处理
	log.Println("🛑 步骤4: 演示消息停止处理功能")

	// // 发布一个特殊消息
	// specialMsgID, err := consumer2.PublishMessage(ctx, "email", map[string]interface{}{
	// 	"to":      "special@example.com",
	// 	"subject": "特殊消息",
	// 	"body":    "这个消息将被停止处理",
	// }, map[string]string{
	// 	"special": "true",
	// })
	// if err != nil {
	// 	log.Printf("发布特殊消息失败: %v", err)
	// } else {
	// log.Printf("📤 发布特殊消息: %s", specialMsgID)

	// // 立即停止处理
	// time.Sleep(time.Second)
	// consumer2.StopMessageProcessing(specialMsgID)
	// consumer3.StopMessageProcessing(specialMsgID)

	// // 等待一会儿
	// time.Sleep(time.Second * 5)

	// // 恢复处理
	// log.Println("🔄 恢复特殊消息处理...")
	// consumer2.ResumeMessageProcessing(specialMsgID)
	// consumer3.ResumeMessageProcessing(specialMsgID)
	// }

	// 等待处理完成
	time.Sleep(time.Second * 10)

	// 清理
	log.Println("🧹 清理资源...")
	consumer2.Stop()
	consumer3.Stop()

	log.Println("✅ Pending消息处理演示完成！")
	log.Println("\n📊 你可以使用以下命令查看Redis Stream状态:")
	log.Println("  make redis-cli")
	log.Println("  然后在Redis CLI中运行:")
	log.Println("    XINFO STREAM pending-demo")
	log.Println("    XINFO GROUPS pending-demo")
	log.Println("    XPENDING pending-demo demo-group")
}
