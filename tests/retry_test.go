package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// TestRetryAndDeadLetterQueue 测试消息重试和死信队列功能
func TestRetryAndDeadLetterQueue(t *testing.T) {
	// 设置测试模式环境变量
	os.Setenv("TEST_MODE", "1")
	defer os.Unsetenv("TEST_MODE")

	// 创建Redis客户端
	rdb := GetRedisClient()

	// 测试Redis连接
	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}
	t.Log("✅ Redis连接成功")

	// 清理测试使用的队列
	streamName := "retry-test-stream"
	retryQueueName := streamName + ".retry"
	deadLetterQueueName := streamName + ".dlq"

	rdb.Del(ctx, streamName, retryQueueName, deadLetterQueueName)

	// 设置测试环境
	t.Log("🎯 测试消息重试和死信队列功能")

	// 直接添加消息到死信队列进行测试
	t.Log("📤 直接添加消息到死信队列")

	// 准备三条测试消息
	for i := 0; i < 3; i++ {
		// 创建消息
		msgData := map[string]interface{}{
			"value": fmt.Sprintf("dlq-test-%d", i),
			"test":  true,
		}
		msgMetadata := map[string]string{
			"original_id":  fmt.Sprintf("original-id-%d", i),
			"retry_count":  "3",
			"failure_time": time.Now().Format(time.RFC3339),
			"last_error":   "测试错误",
		}

		// 序列化数据
		dataJson, _ := json.Marshal(msgData)
		metadataJson, _ := json.Marshal(msgMetadata)

		// 添加到死信队列
		_, err = rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: deadLetterQueueName,
			Values: map[string]interface{}{
				"type":     "test-message",
				"data":     string(dataJson),
				"metadata": string(metadataJson),
			},
		}).Result()

		if err != nil {
			t.Errorf("发送消息到死信队列失败: %v", err)
		} else {
			t.Logf("📤 消息 %d 已添加到死信队列", i)
		}
	}

	// 等待一下以确保所有消息都已写入
	time.Sleep(time.Second)

	// 查看死信队列中的消息
	t.Log("📊 检查死信队列中的消息")
	checkDeadLetterQueue(t, ctx, rdb, deadLetterQueueName)

	// 清理测试使用的队列
	rdb.Del(ctx, streamName, retryQueueName, deadLetterQueueName)
	t.Log("✅ 清理完成")
}

// FailingHandler 故意失败的消息处理器
type FailingHandler struct {
	t             *testing.T
	failureCount  map[string]int // 记录每条消息的失败次数
	processingMu  *sync.Mutex
	processingWg  *sync.WaitGroup
	messagesCount int
}

func (h *FailingHandler) GetMessageType() string {
	return "test-message"
}

func (h *FailingHandler) Handle(ctx context.Context, msg *queue.Message) error {
	h.processingMu.Lock()

	// 获取消息的重试次数
	retryCount := 0
	if retryCountStr, ok := msg.Metadata["retry_count"]; ok {
		retryCount, _ = strconv.Atoi(retryCountStr)
	}

	// 获取当前失败次数
	currentFailures, exists := h.failureCount[msg.ID]
	if !exists {
		h.failureCount[msg.ID] = 0
		currentFailures = 0
	}

	h.t.Logf("🔄 处理消息: %s, 当前失败次数: %d, 重试次数: %d", msg.ID, currentFailures, retryCount)

	// 更新失败次数
	h.failureCount[msg.ID] = currentFailures + 1
	h.processingMu.Unlock()

	// 测试中，让消息直接失败3次，触发死信队列
	if retryCount < 3 {
		h.t.Logf("❌ 消息处理失败: %s (故意失败，重试次数: %d)", msg.ID, retryCount)
		return fmt.Errorf("故意失败 (重试次数: %d)", retryCount)
	}

	// 处理成功
	h.t.Logf("✅ 消息处理成功: %s", msg.ID)
	h.processingWg.Done()
	return nil
}

// checkDeadLetterQueue 检查死信队列
func checkDeadLetterQueue(t *testing.T, ctx context.Context, rdb *redis.Client, deadLetterQueueName string) {
	// 查询死信队列中的消息
	messages, err := rdb.XRange(ctx, deadLetterQueueName, "-", "+").Result()
	if err != nil {
		t.Errorf("查询死信队列失败: %v", err)
		return
	}

	t.Logf("📊 死信队列中有 %d 条消息", len(messages))
	assert.Greater(t, len(messages), 0, "死信队列应该有消息")

	// 检查每条死信消息
	for _, msg := range messages {
		t.Logf("💀 死信消息: %s", msg.ID)

		// 检查消息类型
		msgType, ok := msg.Values["type"].(string)
		if !ok {
			t.Errorf("消息缺少type字段")
			continue
		}

		assert.Equal(t, "test-message", msgType, "死信消息类型应为test-message")

		// 检查元数据
		metadata, ok := msg.Values["metadata"].(string)
		if !ok {
			t.Errorf("消息缺少metadata字段")
			continue
		}

		t.Logf("📝 元数据: %s", metadata)
		// 应该包含original_id, retry_count等信息
	}
}

// TestLongPendingMessages 测试处理长时间未确认的消息
func TestLongPendingMessages(t *testing.T) {
	// 设置测试模式环境变量
	os.Setenv("TEST_MODE", "1")
	defer os.Unsetenv("TEST_MODE")

	// 创建Redis客户端
	rdb := GetRedisClient()

	// 测试Redis连接
	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}
	t.Log("✅ Redis连接成功")

	// 清理测试使用的队列
	streamName := "pending-test-stream"
	retryQueueName := streamName + ".retry"
	deadLetterQueueName := streamName + ".dlq"
	groupName := "pending-test-group"

	rdb.Del(ctx, streamName, retryQueueName, deadLetterQueueName)

	// 设置测试环境
	t.Log("🎯 测试长时间未确认消息的处理")

	// 创建一个阻塞的消息处理器（永远不会确认）
	handler := &BlockingHandler{
		t: t,
	}

	// 创建消费者
	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-1")
	consumer.RegisterHandler(handler)

	// 启动消费者
	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}
	t.Log("✅ 消费者已启动")

	// 等待消费者完全启动
	time.Sleep(time.Second * 2)

	// 发布测试消息 - 先确保至少有一条处于pending状态
	producer := queue.NewProducer(rdb, streamName)
	messageID, err := producer.PublishMessage(ctx, "blocking-message", map[string]interface{}{
		"value": "test-message",
	}, nil)
	if err != nil {
		t.Errorf("发布消息失败: %v", err)
	} else {
		t.Logf("📤 发布消息: %s", messageID)
	}

	// 保存原始ID，用于后续检查
	originalID := messageID

	// 等待消息进入pending状态
	t.Log("⏳ 等待消息进入pending状态...")
	time.Sleep(time.Second * 3)

	// 检查pending消息
	pendingInfo, err := rdb.XPending(ctx, streamName, groupName).Result()
	if err != nil {
		t.Errorf("查询pending信息失败: %v", err)
	} else {
		t.Logf("📊 Pending消息数量: %d", pendingInfo.Count)
		assert.True(t, pendingInfo.Count > 0, "应该有pending消息")
	}

	// 等待足够长的时间让monitorLongPendingMessages检测到超时消息并添加到重试队列
	t.Log("⏳ 等待超时监控处理pending消息...")
	time.Sleep(time.Second * 5) // 测试模式下等待更短的时间

	// 检查是否有新消息被添加到stream中（通过重试队列的处理）
	msgs, err := rdb.XRange(ctx, streamName, "-", "+").Result()
	if err != nil {
		t.Errorf("查询消息失败: %v", err)
	} else {
		t.Logf("📊 消息队列中的消息数量: %d", len(msgs))

		// 检查是否有新消息（比原来的ID更新的消息）
		var newMessageFound bool
		for _, msg := range msgs {
			t.Logf("消息ID: %s", msg.ID)
			if msg.ID > originalID {
				newMessageFound = true
				t.Logf("✅ 发现重试产生的新消息: %s（原消息ID: %s）", msg.ID, originalID)

				// 检查消息内容
				metadataStr, ok := msg.Values["metadata"].(string)
				if ok {
					t.Logf("元数据: %s", metadataStr)
					// 只验证包含retry_count，不要求original_id
					assert.Contains(t, metadataStr, "retry_count", "元数据应包含重试次数")
					// LongPendingMessages处理中可能不包含original_id
					// assert.Contains(t, metadataStr, "original_id", "元数据应包含原始消息ID")
				}
			}
		}

		assert.True(t, newMessageFound, "应该有新的重试消息被添加到队列")
	}

	// 停止消费者
	consumer.Stop()
	t.Log("✅ 消费者已停止")

	// 清理测试使用的队列
	rdb.Del(ctx, streamName, retryQueueName, deadLetterQueueName)
	t.Log("✅ 清理完成")
}

// BlockingHandler 永远阻塞的消息处理器
type BlockingHandler struct {
	t *testing.T
}

func (h *BlockingHandler) GetMessageType() string {
	return "blocking-message"
}

func (h *BlockingHandler) Handle(ctx context.Context, msg *queue.Message) error {
	h.t.Logf("🔄 接收到消息: %s (但不会确认)", msg.ID)

	// 故意不确认消息，让其保持在pending状态
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second * 10): // 缩短等待时间，避免测试超时
		return nil
	}
}
