package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"goStream/queue"
)

// TestConcurrentAutoCleanup 测试多个消费者同时开启自动清理的竞争问题
func TestConcurrentAutoCleanup(t *testing.T) {
	// 创建Redis客户端
	rdb := getRDB()

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "concurrent-cleanup-test"
	groupName := "cleanup-group"

	t.Log("🎯 测试多消费者并发自动清理")

	// 创建多个消费者，都开启自动清理
	numConsumers := 3
	consumers := make([]*queue.MessageQueue, numConsumers)

	// 配置较短的清理间隔以便快速测试
	cleanupPolicy := &queue.CleanupPolicy{
		EnableAutoCleanup: true,
		CleanupInterval:   time.Second * 2,        // 2秒间隔
		MaxStreamLength:   5,                      // 超过5条消息就清理
		MinRetentionTime:  time.Millisecond * 100, // 100ms保留时间
		BatchSize:         10,
	}

	// 启动多个消费者
	var wg sync.WaitGroup
	for i := 0; i < numConsumers; i++ {
		consumerName := fmt.Sprintf("consumer-%d", i)
		consumer := queue.NewMessageQueue(rdb, streamName, groupName, consumerName)
		consumer.GetCleaner().SetCleanupPolicy(cleanupPolicy)

		// 注册处理器
		handler := &ConcurrentTestHandler{t: t, name: consumerName}
		consumer.RegisterHandler(handler)

		consumers[i] = consumer

		wg.Add(1)
		go func(c *queue.MessageQueue, name string) {
			defer wg.Done()
			err := c.Start(ctx)
			if err != nil {
				t.Errorf("启动消费者 %s 失败: %v", name, err)
			}
		}(consumer, consumerName)

		t.Logf("✅ 启动消费者: %s", consumerName)
		time.Sleep(time.Millisecond * 100) // 错开启动时间
	}

	// 等待所有消费者启动
	time.Sleep(time.Second * 2)

	// 创建生产者
	producer := queue.NewProducer(rdb, streamName)

	// 第一轮：发布大量消息，触发清理
	t.Log("📝 第一轮：发布20条消息，触发多次清理")
	for i := 0; i < 20; i++ {
		messageID, err := producer.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("并发清理测试消息 %d", i),
			"round":   1,
		}, map[string]string{
			"test": "concurrent-cleanup",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 200)
	}

	// 等待消息处理和清理
	t.Log("⏳ 等待消息处理和自动清理...")
	time.Sleep(time.Second * 8)

	// 检查Stream状态
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取topic信息失败: %v", err)
	} else {
		t.Logf("📊 第一轮后Stream长度: %d", info.Length)
		for _, group := range info.Groups {
			t.Logf("📊 组 %s: pending=%d", group.Name, group.Pending)
		}
	}

	// 第二轮：再次发布消息，观察清理行为
	t.Log("📝 第二轮：再发布15条消息")
	for i := 0; i < 15; i++ {
		messageID, err := producer.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("第二轮消息 %d", i),
			"round":   2,
		}, map[string]string{
			"test": "concurrent-cleanup-2",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布第二轮消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 150)
	}

	// 等待处理和清理
	time.Sleep(time.Second * 6)

	// 最终检查
	info, err = producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取最终topic信息失败: %v", err)
	} else {
		t.Logf("📊 最终Stream长度: %d", info.Length)
		for _, group := range info.Groups {
			t.Logf("📊 组 %s: pending=%d", group.Name, group.Pending)
		}

		// 验证清理是否正常工作
		if info.Length > cleanupPolicy.MaxStreamLength*2 {
			t.Errorf("⚠️  Stream长度 %d 过大，清理可能没有正常工作", info.Length)
		} else {
			t.Logf("✅ Stream长度控制正常，清理工作正常")
		}
	}

	// 停止所有消费者
	for i, consumer := range consumers {
		consumer.Stop()
		t.Logf("🛑 停止消费者 %d", i)
	}

	t.Log("✅ 并发自动清理测试完成")
}

// TestCleanupRaceCondition 测试清理操作的竞争条件
func TestCleanupRaceCondition(t *testing.T) {
	// 创建Redis客户端
	rdb := getRDB()

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "race-condition-test"
	groupName := "race-group"

	t.Log("🎯 测试清理操作的竞争条件")

	// 创建两个消费者，使用相同的清理配置
	cleanupPolicy := &queue.CleanupPolicy{
		EnableAutoCleanup: false, // 先不启用自动清理
		CleanupInterval:   time.Second * 1,
		MaxStreamLength:   3,
		MinRetentionTime:  time.Millisecond * 50,
		BatchSize:         5,
	}

	consumer1 := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-1")
	consumer2 := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-2")

	consumer1.GetCleaner().SetCleanupPolicy(cleanupPolicy)
	consumer2.GetCleaner().SetCleanupPolicy(cleanupPolicy)

	// 注册处理器
	handler1 := &ConcurrentTestHandler{t: t, name: "consumer-1"}
	handler2 := &ConcurrentTestHandler{t: t, name: "consumer-2"}

	consumer1.RegisterHandler(handler1)
	consumer2.RegisterHandler(handler2)

	// 启动消费者
	err = consumer1.Start(ctx)
	if err != nil {
		t.Fatalf("启动consumer1失败: %v", err)
	}

	err = consumer2.Start(ctx)
	if err != nil {
		t.Fatalf("启动consumer2失败: %v", err)
	}

	t.Log("✅ 两个消费者已启动")

	// 创建生产者并发布消息
	producer := queue.NewProducer(rdb, streamName)

	t.Log("📝 发布10条消息")
	for i := 0; i < 10; i++ {
		messageID, err := producer.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("竞争测试消息 %d", i),
		}, map[string]string{
			"test": "race-condition",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 等待消息处理完成
	time.Sleep(time.Second * 3)

	// 同时触发手动清理，模拟竞争条件
	t.Log("🔄 同时触发两个消费者的手动清理")

	var wg sync.WaitGroup
	var cleaned1, cleaned2 int64
	var err1, err2 error

	wg.Add(2)

	// 消费者1执行清理
	go func() {
		defer wg.Done()
		cleaned1, err1 = consumer1.GetCleaner().CleanupMessages(ctx)
		t.Logf("🧹 消费者1清理结果: %d 条消息, 错误: %v", cleaned1, err1)
	}()

	// 消费者2执行清理（几乎同时）
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 10) // 稍微错开一点
		cleaned2, err2 = consumer2.GetCleaner().CleanupMessages(ctx)
		t.Logf("🧹 消费者2清理结果: %d 条消息, 错误: %v", cleaned2, err2)
	}()

	wg.Wait()

	// 检查结果
	totalCleaned := cleaned1 + cleaned2
	t.Logf("📊 总共清理了 %d 条消息 (消费者1: %d, 消费者2: %d)", totalCleaned, cleaned1, cleaned2)

	if err1 != nil {
		t.Logf("⚠️  消费者1清理错误: %v", err1)
	}
	if err2 != nil {
		t.Logf("⚠️  消费者2清理错误: %v", err2)
	}

	// 检查最终状态
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取最终topic信息失败: %v", err)
	} else {
		t.Logf("📊 最终Stream长度: %d", info.Length)
		for _, group := range info.Groups {
			t.Logf("📊 组 %s: pending=%d", group.Name, group.Pending)
		}
	}

	consumer1.Stop()
	consumer2.Stop()

	t.Log("✅ 竞争条件测试完成")
}

// ConcurrentTestHandler 并发测试处理器
type ConcurrentTestHandler struct {
	t    *testing.T
	name string
}

func (h *ConcurrentTestHandler) GetMessageType() string {
	return "test"
}

func (h *ConcurrentTestHandler) Handle(ctx context.Context, msg *queue.Message) error {
	index := "未知"
	round := "未知"
	if idx, ok := msg.Data["index"]; ok {
		index = fmt.Sprintf("%d", int(idx.(float64)))
	}
	if r, ok := msg.Data["round"]; ok {
		round = fmt.Sprintf("%d", int(r.(float64)))
	}

	h.t.Logf("🔄 [%s] 处理消息: %s (索引: %s, 轮次: %s)", h.name, msg.ID, index, round)

	// 模拟处理时间
	time.Sleep(time.Millisecond * 50)

	h.t.Logf("✅ [%s] 消息处理完成: %s", h.name, msg.ID)
	return nil
}
