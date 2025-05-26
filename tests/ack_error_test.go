package tests

import (
	"context"
	"testing"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// TestACKErrorWhenTopicDeleted 专门测试ACK错误
func TestACKErrorWhenTopicDeleted(t *testing.T) {
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

	streamName := "ack-error-test"
	groupName := "ack-group"

	t.Log("🎯 专门测试ACK错误场景")

	// 启动消费者
	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-1")
	consumer.RegisterHandler(&QuickEmailHandler{t: t})

	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	// 发布一条消息
	producer := queue.NewMessageQueue(rdb, streamName, groupName, "producer")
	messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
		"to":      "test@example.com",
		"subject": "ACK测试",
		"body":    "这条消息将在ACK时遇到错误",
	}, map[string]string{
		"test": "ack-error",
	})

	if err != nil {
		t.Fatalf("发布消息失败: %v", err)
	}

	t.Logf("📤 发布消息: %s", messageID)

	// 等待消息开始处理
	time.Sleep(time.Second * 2)

	// 在消息处理过程中删除topic
	t.Log("🛑 在消息处理过程中删除topic")
	terminator := queue.NewMessageQueue(rdb, streamName, groupName, "terminator")
	err = terminator.TerminateTopic(ctx)
	if err != nil {
		t.Errorf("删除topic失败: %v", err)
	}

	// 等待观察ACK错误
	time.Sleep(time.Second * 3)

	consumer.Stop()
	t.Log("✅ ACK错误测试完成")
}

// QuickEmailHandler 快速邮件处理器，用于测试ACK错误
type QuickEmailHandler struct {
	t *testing.T
}

func (h *QuickEmailHandler) GetMessageType() string {
	return "email"
}

func (h *QuickEmailHandler) Handle(ctx context.Context, msg *queue.Message) error {
	h.t.Logf("⚡ 快速处理邮件消息: %s", msg.ID)

	// 短暂处理时间
	time.Sleep(time.Second * 1)

	h.t.Logf("✅ 邮件处理完成: %s (即将ACK)", msg.ID)
	return nil
}
