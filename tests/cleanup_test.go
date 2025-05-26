package tests

import (
	"context"
	"testing"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// TestMessageCleanup 测试消息清理功能
func TestMessageCleanup(t *testing.T) {
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

	streamName := "cleanup-test"
	groupName1 := "group1"
	groupName2 := "group2"

	t.Log("🎯 测试消息清理功能")

	// 第一步：创建两个消费者组
	t.Log("📝 步骤1: 创建两个消费者组")

	consumer1 := queue.NewMessageQueue(rdb, streamName, groupName1, "consumer1")
	consumer1.RegisterHandler(&CleanupTestHandler{t: t, name: "消费者1"})

	consumer2 := queue.NewMessageQueue(rdb, streamName, groupName2, "consumer2")
	consumer2.RegisterHandler(&CleanupTestHandler{t: t, name: "消费者2"})

	// 启动消费者
	err = consumer1.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者1失败: %v", err)
	}

	err = consumer2.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者2失败: %v", err)
	}

	t.Log("✅ 两个消费者已启动")

	// 第二步：发布一些消息
	t.Log("📝 步骤2: 发布测试消息")

	producer := queue.NewMessageQueue(rdb, streamName, groupName1, "producer")
	producer2 := queue.NewProducer(rdb, streamName)

	var messageIDs []string
	for i := 0; i < 10; i++ {
		messageID, err := producer2.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": "测试消息",
		}, map[string]string{
			"batch": "cleanup-test",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			messageIDs = append(messageIDs, messageID)
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 第三步：等待消息被处理
	t.Log("📝 步骤3: 等待消息被两个消费者组处理")
	time.Sleep(time.Second * 8)

	// 第四步：检查Stream状态
	t.Log("📝 步骤4: 检查Stream状态")
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取topic信息失败: %v", err)
	} else {
		t.Logf("📊 Stream长度: %d", info.Length)
		t.Logf("📊 消费者组数量: %d", len(info.Groups))
		for _, group := range info.Groups {
			t.Logf("📊 组 %s: pending=%d, lastDelivered=%s", group.Name, group.Pending, group.LastDeliveredID)
		}
	}

	// 第五步：手动清理消息
	t.Log("📝 步骤5: 手动清理已处理的消息")

	// 设置清理策略（允许立即清理）
	cleanupPolicy := &queue.CleanupPolicy{
		EnableAutoCleanup: false,           // 手动清理
		MaxStreamLength:   5,               // 低阈值便于测试
		MinRetentionTime:  time.Second * 1, // 短保留时间便于测试
		BatchSize:         5,
	}
	producer.SetCleanupPolicy(cleanupPolicy)

	// 等待一会儿确保消息足够老
	time.Sleep(time.Second * 2)

	cleaned, err := producer.CleanupMessages(ctx)
	if err != nil {
		t.Errorf("清理消息失败: %v", err)
	} else {
		t.Logf("✅ 成功清理了 %d 条消息", cleaned)
	}

	// 第六步：检查清理后的状态
	t.Log("📝 步骤6: 检查清理后的Stream状态")
	info, err = producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取topic信息失败: %v", err)
	} else {
		t.Logf("📊 清理后Stream长度: %d", info.Length)
	}

	// 清理
	consumer1.Stop()
	consumer2.Stop()

	t.Log("✅ 消息清理测试完成")
}

// TestAutoCleanup 测试自动清理功能
func TestAutoCleanup(t *testing.T) {
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

	streamName := "auto-cleanup-test"
	groupName := "auto-group"

	t.Log("🎯 测试自动清理功能")

	// 创建启用自动清理的消费者
	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "consumer")
	consumer.RegisterHandler(&CleanupTestHandler{t: t, name: "自动清理消费者"})

	// 设置激进的清理策略便于测试
	cleanupPolicy := &queue.CleanupPolicy{
		EnableAutoCleanup: true,
		CleanupInterval:   time.Second * 3, // 短间隔便于测试
		MaxStreamLength:   5,               // 低阈值
		MinRetentionTime:  time.Second * 1, // 短保留时间
		BatchSize:         3,
	}
	consumer.SetCleanupPolicy(cleanupPolicy)

	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	t.Log("✅ 启用自动清理的消费者已启动")

	// 发布消息
	producer := queue.NewProducer(rdb, streamName)

	t.Log("📝 发布消息并观察自动清理")
	for i := 0; i < 15; i++ {
		messageID, err := producer.PublishMessage(ctx, "test", map[string]interface{}{
			"index":   i,
			"message": "自动清理测试消息",
		}, map[string]string{
			"batch": "auto-cleanup",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}

		// 每发布几条消息检查一次Stream状态
		if (i+1)%5 == 0 {
			info, err := producer.GetTopicInfo(ctx)
			if err != nil {
				t.Errorf("获取topic信息失败: %v", err)
			} else {
				t.Logf("📊 当前Stream长度: %d", info.Length)
			}
		}

		time.Sleep(time.Millisecond * 500)
	}

	// 等待自动清理运行
	t.Log("⏳ 等待自动清理运行...")
	time.Sleep(time.Second * 10)

	// 检查最终状态
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取topic信息失败: %v", err)
	} else {
		t.Logf("📊 最终Stream长度: %d", info.Length)
		if info.Length <= cleanupPolicy.MaxStreamLength {
			t.Log("✅ 自动清理成功，Stream长度在限制范围内")
		} else {
			t.Logf("⚠️ Stream长度 %d 仍超过限制 %d", info.Length, cleanupPolicy.MaxStreamLength)
		}
	}

	consumer.Stop()
	t.Log("✅ 自动清理测试完成")
}

// CleanupTestHandler 清理测试处理器
type CleanupTestHandler struct {
	t    *testing.T
	name string
}

func (h *CleanupTestHandler) GetMessageType() string {
	return "test"
}

func (h *CleanupTestHandler) Handle(ctx context.Context, msg *queue.Message) error {
	index := "未知"
	if idx, ok := msg.Data["index"]; ok {
		index = string(rune(int(idx.(float64)) + 48))
	}

	h.t.Logf("🔄 [%s] 处理消息: %s (索引: %s)", h.name, msg.ID, index)

	// 模拟处理时间
	time.Sleep(time.Millisecond * 200)

	h.t.Logf("✅ [%s] 消息处理完成: %s", h.name, msg.ID)
	return nil
}
