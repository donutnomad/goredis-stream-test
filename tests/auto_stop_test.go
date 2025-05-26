package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// TestConsumerAutoStop 测试消费者自动停止功能
func TestConsumerAutoStop(t *testing.T) {
	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "auto-stop-test"
	groupName := "auto-stop-group"

	t.Log("🎯 测试消费者自动停止功能")

	// 清理可能存在的旧数据
	rdb.Del(ctx, streamName)
	time.Sleep(time.Millisecond * 100)

	// 创建消费者
	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "auto-stop-consumer")
	consumer.RegisterHandler(&AutoStopTestHandler{t: t})

	// 启动消费者
	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}
	t.Log("✅ 消费者已启动")

	// 等待消费者完全启动
	time.Sleep(time.Second * 1)

	// 发布一些消息
	producer := queue.NewProducer(rdb, streamName)
	for i := 0; i < 3; i++ {
		messageID, err := producer.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": "自动停止测试消息",
		}, map[string]string{
			"test": "auto-stop",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 200)
	}

	// 等待消息处理
	time.Sleep(time.Second * 2)

	// 使用WaitGroup来同步测试
	var wg sync.WaitGroup

	// 启动监控goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Log("👀 开始监控消费者自动停止...")

		select {
		case <-consumer.Done():
			t.Log("✅ 消费者已自动停止")
			if consumer.IsAutoStopped() {
				t.Log("✅ 确认消费者是因为topic删除而自动停止")
			} else {
				t.Log("⚠️  消费者停止但不是自动停止")
			}
		case <-time.After(time.Second * 10):
			t.Error("❌ 超时：消费者未在预期时间内自动停止")
		}
	}()

	// 等待一会儿，然后删除topic
	time.Sleep(time.Second * 1)
	t.Log("🛑 删除topic以触发自动停止...")

	// 删除topic
	terminator := queue.NewMessageQueue(rdb, streamName, groupName, "terminator")
	err = terminator.TerminateTopic(ctx)
	if err != nil {
		t.Errorf("删除topic失败: %v", err)
	}

	// 等待所有goroutine完成
	wg.Wait()

	// 验证消费者状态
	if !consumer.IsAutoStopped() {
		t.Error("❌ 消费者应该处于自动停止状态")
	}

	t.Log("✅ 自动停止功能测试完成")
}

// TestMultipleConsumersAutoStop 测试多个消费者的自动停止
func TestMultipleConsumersAutoStop(t *testing.T) {
	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "multi-auto-stop-test"
	groupName := "multi-auto-stop-group"

	t.Log("🎯 测试多个消费者的自动停止功能")

	// 清理可能存在的旧数据
	rdb.Del(ctx, streamName)
	time.Sleep(time.Millisecond * 100)

	// 创建多个消费者
	numConsumers := 3
	consumers := make([]*queue.MessageQueue, numConsumers)

	for i := 0; i < numConsumers; i++ {
		consumerName := fmt.Sprintf("consumer-%d", i)
		consumer := queue.NewMessageQueue(rdb, streamName, groupName, consumerName)
		consumer.RegisterHandler(&AutoStopTestHandler{t: t, name: consumerName})

		err := consumer.Start(ctx)
		if err != nil {
			t.Fatalf("启动消费者 %s 失败: %v", consumerName, err)
		}

		consumers[i] = consumer
		t.Logf("✅ 消费者 %s 已启动", consumerName)
		time.Sleep(time.Millisecond * 200)
	}

	// 等待所有消费者启动
	time.Sleep(time.Second * 1)

	// 发布一些消息
	producer := queue.NewProducer(rdb, streamName)
	for i := 0; i < 5; i++ {
		messageID, err := producer.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": "多消费者自动停止测试",
		}, map[string]string{
			"test": "multi-auto-stop",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 等待消息处理
	time.Sleep(time.Second * 2)

	// 使用WaitGroup监控所有消费者
	var wg sync.WaitGroup

	// 为每个消费者启动监控goroutine
	for i, consumer := range consumers {
		wg.Add(1)
		go func(idx int, c *queue.MessageQueue) {
			defer wg.Done()
			consumerName := fmt.Sprintf("consumer-%d", idx)
			t.Logf("👀 开始监控消费者 %s 的自动停止...", consumerName)

			select {
			case <-c.Done():
				t.Logf("✅ 消费者 %s 已自动停止", consumerName)
				if c.IsAutoStopped() {
					t.Logf("✅ 确认消费者 %s 是因为topic删除而自动停止", consumerName)
				} else {
					t.Logf("⚠️  消费者 %s 停止但不是自动停止", consumerName)
				}
			case <-time.After(time.Second * 10):
				t.Errorf("❌ 超时：消费者 %s 未在预期时间内自动停止", consumerName)
			}
		}(i, consumer)
	}

	// 等待一会儿，然后删除topic
	time.Sleep(time.Second * 1)
	t.Log("🛑 删除topic以触发所有消费者自动停止...")

	// 删除topic
	terminator := queue.NewMessageQueue(rdb, streamName, groupName, "terminator")
	err = terminator.TerminateTopic(ctx)
	if err != nil {
		t.Errorf("删除topic失败: %v", err)
	}

	// 等待所有监控goroutine完成
	wg.Wait()

	// 验证所有消费者状态
	for i, consumer := range consumers {
		if !consumer.IsAutoStopped() {
			t.Errorf("❌ 消费者 %d 应该处于自动停止状态", i)
		}
	}

	t.Log("✅ 多消费者自动停止功能测试完成")
}

// TestAutoStopVsManualStop 测试自动停止与手动停止的区别
func TestAutoStopVsManualStop(t *testing.T) {
	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	t.Log("🎯 测试自动停止与手动停止的区别")

	// 测试1：手动停止
	t.Log("📝 测试1：手动停止消费者")
	streamName1 := "manual-stop-test"
	groupName1 := "manual-stop-group"

	rdb.Del(ctx, streamName1)
	time.Sleep(time.Millisecond * 100)

	consumer1 := queue.NewMessageQueue(rdb, streamName1, groupName1, "manual-consumer")
	consumer1.RegisterHandler(&AutoStopTestHandler{t: t, name: "manual-consumer"})

	err = consumer1.Start(ctx)
	if err != nil {
		t.Fatalf("启动手动测试消费者失败: %v", err)
	}

	time.Sleep(time.Second * 1)

	// 手动停止
	consumer1.Stop()

	// 检查状态
	if consumer1.IsAutoStopped() {
		t.Error("❌ 手动停止的消费者不应该标记为自动停止")
	} else {
		t.Log("✅ 手动停止的消费者正确地未标记为自动停止")
	}

	// 测试2：自动停止
	t.Log("📝 测试2：自动停止消费者")
	streamName2 := "auto-stop-test-2"
	groupName2 := "auto-stop-group-2"

	rdb.Del(ctx, streamName2)
	time.Sleep(time.Millisecond * 100)

	consumer2 := queue.NewMessageQueue(rdb, streamName2, groupName2, "auto-consumer")
	consumer2.RegisterHandler(&AutoStopTestHandler{t: t, name: "auto-consumer"})

	err = consumer2.Start(ctx)
	if err != nil {
		t.Fatalf("启动自动测试消费者失败: %v", err)
	}

	time.Sleep(time.Second * 1)

	// 删除topic触发自动停止
	terminator := queue.NewMessageQueue(rdb, streamName2, groupName2, "terminator")
	err = terminator.TerminateTopic(ctx)
	if err != nil {
		t.Errorf("删除topic失败: %v", err)
	}

	// 等待自动停止
	select {
	case <-consumer2.Done():
		t.Log("✅ 消费者已自动停止")
	case <-time.After(time.Second * 5):
		t.Error("❌ 消费者未在预期时间内自动停止")
	}

	// 检查状态
	if !consumer2.IsAutoStopped() {
		t.Error("❌ 自动停止的消费者应该标记为自动停止")
	} else {
		t.Log("✅ 自动停止的消费者正确地标记为自动停止")
	}

	t.Log("✅ 自动停止与手动停止区别测试完成")
}

// AutoStopTestHandler 自动停止测试处理器
type AutoStopTestHandler struct {
	t    *testing.T
	name string
}

func (h *AutoStopTestHandler) GetMessageType() string {
	return "test"
}

func (h *AutoStopTestHandler) Handle(ctx context.Context, msg *queue.Message) error {
	consumerName := h.name
	if consumerName == "" {
		consumerName = "unknown"
	}

	index := "未知"
	if idx, ok := msg.Data["index"]; ok {
		index = fmt.Sprintf("%d", int(idx.(float64)))
	}

	h.t.Logf("🔄 [%s] 处理消息: %s (索引: %s)", consumerName, msg.ID, index)

	// 模拟处理时间
	time.Sleep(time.Millisecond * 100)

	h.t.Logf("✅ [%s] 消息处理完成: %s", consumerName, msg.ID)
	return nil
}
