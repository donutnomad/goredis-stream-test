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

// TestCoordinatedCleanup 测试协调清理功能
func TestCoordinatedCleanup(t *testing.T) {
	// 创建Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "123456",
		DB:       0,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "coordinated-cleanup-test"
	groupName := "coord-group"

	t.Log("🎯 测试协调清理功能")

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
		consumerName := fmt.Sprintf("coord-consumer-%d", i)
		consumer := queue.NewMessageQueue(rdb, streamName, groupName, consumerName)
		consumer.SetCleanupPolicy(cleanupPolicy)

		// 注册处理器
		handler := &CoordinatedTestHandler{t: t, name: consumerName}
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

		t.Logf("✅ 启动协调消费者: %s", consumerName)
		time.Sleep(time.Millisecond * 100) // 错开启动时间
	}

	// 等待所有消费者启动
	time.Sleep(time.Second * 2)

	// 创建生产者
	producer := queue.NewProducer(rdb, streamName)

	// 发布大量消息，触发协调清理
	t.Log("📝 发布20条消息，触发协调清理")
	for i := 0; i < 20; i++ {
		messageID, err := producer.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("协调清理测试消息 %d", i),
		}, map[string]string{
			"test": "coordinated-cleanup",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 200)
	}

	// 等待消息处理和协调清理
	t.Log("⏳ 等待消息处理和协调清理...")
	time.Sleep(time.Second * 8)

	// 检查清理统计
	coordinator := queue.NewCleanupCoordinator(rdb, streamName)
	stats, err := coordinator.GetCleanupStats(ctx)
	if err != nil {
		t.Errorf("获取清理统计失败: %v", err)
	} else {
		t.Logf("📊 清理统计: %+v", stats.Stats)
	}

	// 检查Stream状态
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取topic信息失败: %v", err)
	} else {
		t.Logf("📊 最终Stream长度: %d", info.Length)
		for _, group := range info.Groups {
			t.Logf("📊 组 %s: pending=%d", group.Name, group.Pending)
		}

		// 验证协调清理是否正常工作
		if info.Length > cleanupPolicy.MaxStreamLength*2 {
			t.Errorf("⚠️  Stream长度 %d 过大，协调清理可能没有正常工作", info.Length)
		} else {
			t.Logf("✅ Stream长度控制正常，协调清理工作正常")
		}
	}

	// 停止所有消费者
	for i, consumer := range consumers {
		consumer.Stop()
		t.Logf("🛑 停止协调消费者 %d", i)
	}

	t.Log("✅ 协调清理测试完成")
}

//// TestCleanupCoordinator 测试清理协调器的基本功能
//func TestCleanupCoordinator(t *testing.T) {
//	// 创建Redis客户端
//	rdb := redis.NewClient(&redis.Options{
//		Addr:     "localhost:6379",
//		Password: "",
//		DB:       0,
//	})
//
//	ctx := context.Background()
//	_, err := rdb.Ping(ctx).Result()
//	if err != nil {
//		t.Skipf("跳过测试：无法连接到Redis: %v", err)
//		return
//	}
//
//	streamName := "coordinator-test"
//	coordinator := queue.NewCleanupCoordinator(rdb, streamName)
//
//	t.Log("🎯 测试清理协调器基本功能")
//
//	// 测试获取锁
//	t.Log("🔒 测试获取清理锁")
//	acquired1, err := coordinator.TryAcquireCleanupLock(ctx, "consumer-1")
//	if err != nil {
//		t.Fatalf("获取锁失败: %v", err)
//	}
//	if !acquired1 {
//		t.Fatalf("应该能够获取锁")
//	}
//	t.Log("✅ consumer-1 成功获取锁")
//
//	// 测试锁的排他性
//	t.Log("🔒 测试锁的排他性")
//	acquired2, err := coordinator.TryAcquireCleanupLock(ctx, "consumer-2")
//	if err != nil {
//		t.Fatalf("尝试获取锁失败: %v", err)
//	}
//	if acquired2 {
//		t.Fatalf("不应该能够获取已被持有的锁")
//	}
//	t.Log("✅ consumer-2 正确地无法获取已被持有的锁")
//
//	// 测试检查清理状态
//	t.Log("🔍 测试检查清理状态")
//	inProgress, holder, err := coordinator.IsCleanupInProgress(ctx)
//	if err != nil {
//		t.Fatalf("检查清理状态失败: %v", err)
//	}
//	if !inProgress {
//		t.Fatalf("应该显示清理正在进行")
//	}
//	if holder != "consumer-1" {
//		t.Fatalf("锁持有者应该是 consumer-1，实际是 %s", holder)
//	}
//	t.Logf("✅ 清理状态正确: 进行中=%v, 持有者=%s", inProgress, holder)
//
//	// 测试延长锁TTL
//	t.Log("⏰ 测试延长锁TTL")
//	err = coordinator.ExtendCleanupLock(ctx, "consumer-1")
//	if err != nil {
//		t.Fatalf("延长锁TTL失败: %v", err)
//	}
//	t.Log("✅ 成功延长锁TTL")
//
//	// 测试非持有者无法延长锁
//	t.Log("⏰ 测试非持有者无法延长锁")
//	err = coordinator.ExtendCleanupLock(ctx, "consumer-2")
//	if err != nil {
//		t.Fatalf("延长锁TTL操作失败: %v", err)
//	}
//	t.Log("✅ 非持有者正确地无法延长锁")
//
//	// 测试更新统计信息
//	t.Log("📊 测试更新清理统计")
//	err = coordinator.UpdateCleanupStats(ctx, "consumer-1", 10)
//	if err != nil {
//		t.Fatalf("更新统计失败: %v", err)
//	}
//	t.Log("✅ 成功更新清理统计")
//
//	// 测试获取统计信息
//	t.Log("📊 测试获取清理统计")
//	stats, err := coordinator.GetCleanupStats(ctx)
//	if err != nil {
//		t.Fatalf("获取统计失败: %v", err)
//	}
//	t.Logf("✅ 获取统计成功: %+v", stats.Stats)
//
//	// 测试释放锁
//	t.Log("🔓 测试释放锁")
//	err = coordinator.ReleaseCleanupLock(ctx, "consumer-1")
//	if err != nil {
//		t.Fatalf("释放锁失败: %v", err)
//	}
//	t.Log("✅ consumer-1 成功释放锁")
//
//	// 测试锁释放后其他消费者可以获取
//	t.Log("🔒 测试锁释放后其他消费者可以获取")
//	acquired3, err := coordinator.TryAcquireCleanupLock(ctx, "consumer-2")
//	if err != nil {
//		t.Fatalf("获取锁失败: %v", err)
//	}
//	if !acquired3 {
//		t.Fatalf("应该能够获取已释放的锁")
//	}
//	t.Log("✅ consumer-2 成功获取已释放的锁")
//
//	// 清理
//	err = coordinator.ReleaseCleanupLock(ctx, "consumer-2")
//	if err != nil {
//		t.Fatalf("清理锁失败: %v", err)
//	}
//
//	t.Log("✅ 清理协调器基本功能测试完成")
//}

// CoordinatedTestHandler 协调测试处理器
type CoordinatedTestHandler struct {
	t    *testing.T
	name string
}

func (h *CoordinatedTestHandler) GetMessageType() string {
	return "test"
}

func (h *CoordinatedTestHandler) Handle(ctx context.Context, msg *queue.Message) error {
	index := "未知"
	if idx, ok := msg.Data["index"]; ok {
		index = fmt.Sprintf("%d", int(idx.(float64)))
	}

	h.t.Logf("🔄 [%s] 处理消息: %s (索引: %s)", h.name, msg.ID, index)

	// 模拟处理时间
	time.Sleep(time.Millisecond * 50)

	h.t.Logf("✅ [%s] 消息处理完成: %s", h.name, msg.ID)
	return nil
}
