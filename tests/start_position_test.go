package tests

import (
	"context"
	"testing"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// TestStartFromEarliest 测试从最早位置开始消费
func TestStartFromEarliest(t *testing.T) {
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

	streamName := "start-position-test"
	groupName := "earliest-group"

	t.Log("🎯 测试从最早位置开始消费")

	// 第一步：先发布一些消息（在创建消费者之前）
	t.Log("📝 步骤1: 先发布历史消息")

	producer := queue.NewProducer(rdb, streamName)

	var historicalMessages []string
	for i := 0; i < 3; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "历史邮件",
			"body":    "这是历史消息",
			"index":   i,
		}, map[string]string{
			"type": "historical",
		})
		if err != nil {
			t.Errorf("发布历史消息失败: %v", err)
		} else {
			historicalMessages = append(historicalMessages, messageID)
			t.Logf("📤 发布历史消息: %s", messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 第二步：创建从最早位置开始的消费者
	t.Log("🔄 步骤2: 创建从最早位置开始的消费者")

	consumer := queue.NewMessageQueueWithStartPosition(rdb, streamName, groupName, "consumer-earliest", queue.StartFromEarliest)
	consumer.RegisterHandler(&HistoryEmailHandler{t: t})

	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	t.Log("✅ 消费者已启动，应该能处理历史消息")

	// 第三步：发布新消息
	t.Log("📝 步骤3: 发布新消息")

	time.Sleep(time.Second * 2) // 等待消费者处理历史消息

	for i := 0; i < 2; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "新邮件",
			"body":    "这是新消息",
			"index":   i + 100,
		}, map[string]string{
			"type": "new",
		})
		if err != nil {
			t.Errorf("发布新消息失败: %v", err)
		} else {
			t.Logf("📤 发布新消息: %s", messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 等待所有消息处理完成
	t.Log("⏳ 等待所有消息处理完成...")
	time.Sleep(time.Second * 8)

	consumer.Stop()
	t.Log("✅ 从最早位置消费测试完成")
}

// TestStartFromLatest 测试从最新位置开始消费（对比测试）
func TestStartFromLatest(t *testing.T) {
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

	streamName := "latest-position-test"
	groupName := "latest-group"

	t.Log("🎯 测试从最新位置开始消费（对比测试）")

	// 第一步：先发布一些消息（在创建消费者之前）
	t.Log("📝 步骤1: 先发布历史消息")

	producer := queue.NewProducer(rdb, streamName)

	for i := 0; i < 3; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "历史邮件",
			"body":    "这是历史消息",
			"index":   i,
		}, map[string]string{
			"type": "historical",
		})
		if err != nil {
			t.Errorf("发布历史消息失败: %v", err)
		} else {
			t.Logf("📤 发布历史消息: %s", messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 第二步：创建从最新位置开始的消费者（默认行为）
	t.Log("🔄 步骤2: 创建从最新位置开始的消费者")

	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-latest")
	consumer.RegisterHandler(&HistoryEmailHandler{t: t})

	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	t.Log("✅ 消费者已启动，应该不会处理历史消息")

	// 第三步：发布新消息
	t.Log("📝 步骤3: 发布新消息")

	time.Sleep(time.Second * 2) // 等待消费者启动

	for i := 0; i < 2; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "新邮件",
			"body":    "这是新消息",
			"index":   i + 100,
		}, map[string]string{
			"type": "new",
		})
		if err != nil {
			t.Errorf("发布新消息失败: %v", err)
		} else {
			t.Logf("📤 发布新消息: %s", messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 等待新消息处理完成
	t.Log("⏳ 等待新消息处理完成...")
	time.Sleep(time.Second * 5)

	consumer.Stop()
	t.Log("✅ 从最新位置消费测试完成")
}

// TestStartFromSpecificID 测试从指定消息ID开始消费
func TestStartFromSpecificID(t *testing.T) {
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

	streamName := "specific-position-test"
	groupName := "specific-group"

	t.Log("🎯 测试从指定消息ID开始消费")

	// 第一步：发布一些消息
	t.Log("📝 步骤1: 发布消息并记录中间的消息ID")

	producer := queue.NewProducer(rdb, streamName)

	var messageIDs []string
	for i := 0; i < 5; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "测试邮件",
			"body":    "这是测试消息",
			"index":   i,
		}, map[string]string{
			"sequence": string(rune(i + 48)), // 转换为字符
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			messageIDs = append(messageIDs, messageID)
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 选择从第3条消息开始消费（索引2）
	if len(messageIDs) < 3 {
		t.Fatal("消息数量不足")
	}

	startFromID := messageIDs[2]
	t.Logf("🎯 将从消息ID %s 开始消费", startFromID)

	// 第二步：创建从指定ID开始的消费者
	t.Log("🔄 步骤2: 创建从指定ID开始的消费者")

	consumer := queue.NewMessageQueueFromSpecificID(rdb, streamName, groupName, "consumer-specific", startFromID)
	consumer.RegisterHandler(&HistoryEmailHandler{t: t})

	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	t.Log("✅ 消费者已启动，应该从指定ID开始处理消息")

	// 等待消息处理完成
	t.Log("⏳ 等待消息处理完成...")
	time.Sleep(time.Second * 8)

	consumer.Stop()
	t.Log("✅ 从指定ID消费测试完成")
}

// HistoryEmailHandler 历史邮件处理器，用于测试不同开始位置
type HistoryEmailHandler struct {
	t *testing.T
}

func (h *HistoryEmailHandler) GetMessageType() string {
	return "email"
}

func (h *HistoryEmailHandler) Handle(ctx context.Context, msg *queue.Message) error {
	msgType := "未知"
	if metadata, ok := msg.Metadata["type"]; ok {
		msgType = metadata
	}

	index := "未知"
	if idx, ok := msg.Data["index"]; ok {
		index = string(rune(int(idx.(float64)) + 48))
	}

	h.t.Logf("📧 处理%s消息: %s (索引: %s)", msgType, msg.ID, index)

	// 模拟处理时间
	time.Sleep(time.Millisecond * 500)

	h.t.Logf("✅ %s消息处理完成: %s", msgType, msg.ID)
	return nil
}
