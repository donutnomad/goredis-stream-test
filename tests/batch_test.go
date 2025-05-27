package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"goStream/queue"
)

// TestBatchProcessing 测试批量处理功能
func TestBatchProcessing(t *testing.T) {
	// 创建Redis客户端
	rdb := getRDB()

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "batch-test"
	groupName := "batch-group"

	t.Log("🎯 测试批量处理功能")

	// 创建消费者
	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "batch-consumer")

	// 配置批量处理
	batchConfig := &queue.BatchConfig{
		EnableBatch:  true,
		BatchSize:    5,                      // 批量大小为5
		BatchTimeout: time.Millisecond * 500, // 500ms超时
		MaxWaitTime:  time.Second * 2,        // 最大等待2秒
	}
	consumer.SetBatchConfig(batchConfig)

	// 注册批量处理器
	batchHandler := &BatchTestHandler{t: t, name: "批量处理器"}
	consumer.RegisterBatchHandler(batchHandler)

	// 启动消费者
	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	t.Log("✅ 批量处理消费者已启动")

	// 创建生产者
	producer := queue.NewProducer(rdb, streamName)

	// 第一轮：发布正好一批的消息（5条）
	t.Log("📝 第一轮：发布5条消息（正好一批）")
	for i := 0; i < 5; i++ {
		messageID, err := producer.PublishMessage(ctx, "batch-test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("批量测试消息 %d", i),
			"round":   1,
		}, map[string]string{
			"batch": "round-1",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 等待处理
	time.Sleep(time.Second * 2)

	// 第二轮：发布超过一批的消息（12条）
	t.Log("📝 第二轮：发布12条消息（超过一批）")
	for i := 0; i < 12; i++ {
		messageID, err := producer.PublishMessage(ctx, "batch-test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("批量测试消息 %d", i),
			"round":   2,
		}, map[string]string{
			"batch": "round-2",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 50)
	}

	// 等待处理
	time.Sleep(time.Second * 3)

	// 第三轮：发布少于一批的消息（3条），测试超时处理
	t.Log("📝 第三轮：发布3条消息（少于一批，测试超时）")
	for i := 0; i < 3; i++ {
		messageID, err := producer.PublishMessage(ctx, "batch-test", map[string]any{
			"index":   i,
			"message": fmt.Sprintf("批量测试消息 %d", i),
			"round":   3,
		}, map[string]string{
			"batch": "round-3",
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 等待超时处理
	t.Log("⏳ 等待超时处理...")
	time.Sleep(time.Second * 3)

	// 检查Stream状态
	info, err := producer.GetTopicInfo(ctx)
	if err != nil {
		t.Errorf("获取topic信息失败: %v", err)
	} else {
		t.Logf("📊 最终Stream长度: %d", info.Length)
		for _, group := range info.Groups {
			t.Logf("📊 组 %s: pending=%d", group.Name, group.Pending)
		}
	}

	consumer.Stop()
	t.Log("✅ 批量处理测试完成")
}

// TestMixedProcessing 测试混合处理（批量+单条）
func TestMixedProcessing(t *testing.T) {
	// 创建Redis客户端
	rdb := getRDB()

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "mixed-test"
	groupName := "mixed-group"

	t.Log("🎯 测试混合处理功能")

	// 创建消费者
	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "mixed-consumer")

	// 配置批量处理
	batchConfig := &queue.BatchConfig{
		EnableBatch:  true,
		BatchSize:    3,
		BatchTimeout: time.Millisecond * 300,
		MaxWaitTime:  time.Second * 1,
	}
	consumer.SetBatchConfig(batchConfig)

	// 注册批量处理器（只处理batch-type类型）
	batchHandler := &BatchTestHandler{t: t, name: "批量处理器"}
	consumer.RegisterBatchHandler(batchHandler)

	// 注册单条处理器（处理single-type类型）
	singleHandler := &SingleTestHandler{t: t, name: "单条处理器"}
	consumer.RegisterHandler(singleHandler)

	// 启动消费者
	err = consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	t.Log("✅ 混合处理消费者已启动")

	// 创建生产者
	producer := queue.NewProducer(rdb, streamName)

	// 发布混合类型的消息
	t.Log("📝 发布混合类型的消息")

	// 发布批量处理类型的消息
	for i := 0; i < 6; i++ {
		messageID, err := producer.PublishMessage(ctx, "batch-test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("批量消息 %d", i),
		}, map[string]string{
			"type": "batch",
		})
		if err != nil {
			t.Errorf("发布批量消息失败: %v", err)
		} else {
			t.Logf("📤 发布批量消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 100)
	}

	// 发布单条处理类型的消息
	for i := 0; i < 4; i++ {
		messageID, err := producer.PublishMessage(ctx, "single-test", map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("单条消息 %d", i),
		}, map[string]string{
			"type": "single",
		})
		if err != nil {
			t.Errorf("发布单条消息失败: %v", err)
		} else {
			t.Logf("📤 发布单条消息 %d: %s", i, messageID)
		}
		time.Sleep(time.Millisecond * 150)
	}

	// 等待处理
	time.Sleep(time.Second * 4)

	consumer.Stop()
	t.Log("✅ 混合处理测试完成")
}

// BatchTestHandler 批量测试处理器
type BatchTestHandler struct {
	t    *testing.T
	name string
}

func (h *BatchTestHandler) GetMessageType() string {
	return "batch-test"
}

func (h *BatchTestHandler) GetBatchSize() int {
	return 0 // 使用全局配置
}

func (h *BatchTestHandler) HandleBatch(ctx context.Context, messages []*queue.Message) error {
	h.t.Logf("🔄 [%s] 开始批量处理 %d 条消息", h.name, len(messages))

	for i, msg := range messages {
		index := "未知"
		round := "未知"
		if idx, ok := msg.Data["index"]; ok {
			index = fmt.Sprintf("%d", int(idx.(float64)))
		}
		if r, ok := msg.Data["round"]; ok {
			round = fmt.Sprintf("%d", int(r.(float64)))
		}

		h.t.Logf("🔄 [%s] 处理消息 %d/%d: %s (索引: %s, 轮次: %s)",
			h.name, i+1, len(messages), msg.ID, index, round)
	}

	// 模拟批量处理时间
	time.Sleep(time.Millisecond * 200)

	h.t.Logf("✅ [%s] 批量处理完成，共处理 %d 条消息", h.name, len(messages))
	return nil
}

// SingleTestHandler 单条测试处理器
type SingleTestHandler struct {
	t    *testing.T
	name string
}

func (h *SingleTestHandler) GetMessageType() string {
	return "single-test"
}

func (h *SingleTestHandler) Handle(ctx context.Context, msg *queue.Message) error {
	index := "未知"
	if idx, ok := msg.Data["index"]; ok {
		index = fmt.Sprintf("%d", int(idx.(float64)))
	}

	h.t.Logf("🔄 [%s] 处理单条消息: %s (索引: %s)", h.name, msg.ID, index)

	// 模拟处理时间
	time.Sleep(time.Millisecond * 100)

	h.t.Logf("✅ [%s] 单条消息处理完成: %s", h.name, msg.ID)
	return nil
}
