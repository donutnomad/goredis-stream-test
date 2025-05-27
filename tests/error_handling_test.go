package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"goStream/queue"

	"github.com/redis/go-redis/v9"
)

// TestErrorHandlingWhenTopicDeleted 测试topic删除时的错误处理
func TestErrorHandlingWhenTopicDeleted(t *testing.T) {
	// 创建Redis客户端
	rdb := getRDB()

	// 测试Redis连接
	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}
	t.Log("✅ Redis连接成功")

	// 演示错误处理
	demonstrateErrorHandling(t, ctx, rdb)
}

func demonstrateErrorHandling(t *testing.T, ctx context.Context, rdb *redis.Client) {
	streamName := "error-demo-test"
	groupName := "error-group-test"

	a, b := rdb.Del(ctx, streamName).Result()
	t.Logf("Del结果: %v, %v", a, b)

	time.Sleep(time.Second * 10)

	t.Log("🎯 演示Topic删除时的错误处理")

	// 第一步：准备测试环境
	t.Log("📝 步骤1: 准备测试环境")

	// 第二步：先启动消费者
	t.Log("🔄 步骤2: 先启动消费者")

	consumer := queue.NewMessageQueue(rdb, streamName, groupName, "consumer-1")
	consumer.RegisterHandler(&SlowEmailHandler{t: t}) // 使用慢处理器

	err := consumer.Start(ctx)
	if err != nil {
		t.Fatalf("启动消费者失败: %v", err)
	}

	t.Log("✅ 消费者已启动")

	// 等待消费者完全启动
	time.Sleep(time.Second * 2)

	// 第三步：发布消息（在消费者启动后）
	t.Log("📝 步骤3: 发布消息供消费者处理")

	producer := queue.NewProducer(rdb, streamName)

	// 发布一些消息
	for i := 0; i < 3; i++ {
		messageID, err := producer.PublishMessage(ctx, "email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "测试邮件",
			"body":    "这是一个测试邮件",
		}, map[string]string{
			"index": string(rune(i + 48)), // 转换为字符
		})
		if err != nil {
			t.Errorf("发布消息失败: %v", err)
		} else {
			t.Logf("📤 发布消息: %s", messageID)
		}
		time.Sleep(time.Millisecond * 500) // 间隔发布
	}

	// 让消费者开始处理消息
	t.Log("⏳ 等待消费者处理消息...")
	time.Sleep(time.Second * 4)

	// 第四步：在消费者处理过程中删除topic
	t.Log("🛑 步骤4: 在消费者处理过程中删除topic")

	var wg sync.WaitGroup

	// 启动一个goroutine来监控消费者的错误
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitorConsumerErrors(t, ctx, consumer, streamName, groupName)
	}()

	// 启动一个goroutine来监控消费者的自动停止
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
		case <-time.After(time.Second * 15):
			t.Log("⚠️  超时：消费者未在预期时间内自动停止")
		}
	}()

	// 等待一会儿，然后删除topic
	time.Sleep(time.Second * 2)

	t.Log("⚠️  即将删除topic，观察消费者的自动停止...")

	// 使用另一个实例来删除topic
	terminator := queue.NewMessageQueue(rdb, streamName, groupName, "terminator")
	err = terminator.TerminateTopic(ctx)
	if err != nil {
		t.Errorf("删除topic失败: %v", err)
	}

	// 等待消费者自动停止
	t.Log("⏳ 等待消费者自动停止...")
	select {
	case <-consumer.Done():
		t.Log("11✅ 消费者已自动停止")
	case <-time.After(time.Second * 10):
		t.Log("11⚠️  消费者未在预期时间内自动停止，手动停止")
		consumer.Stop()
	}

	wg.Wait()

	t.Log("✅ 错误处理演示完成！")
}

// SlowEmailHandler 慢速邮件处理器，用于演示长时间处理
type SlowEmailHandler struct {
	t *testing.T
}

func (h *SlowEmailHandler) GetMessageType() string {
	return "email"
}

func (h *SlowEmailHandler) Handle(ctx context.Context, msg *queue.Message) error {
	h.t.Logf("🐌 开始慢速处理邮件消息: %s", msg.ID)

	// 模拟长时间处理
	time.Sleep(time.Second * 6)

	h.t.Logf("✅ 邮件处理完成: %s", msg.ID)
	return nil
}

// monitorConsumerErrors 监控消费者的错误
func monitorConsumerErrors(t *testing.T, ctx context.Context, consumer *queue.MessageQueue, streamName, groupName string) {
	t.Log("👀 开始监控消费者错误...")

	// 创建一个新的Redis客户端来监控
	rdb := getRDB()

	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	for i := 0; i < 5; i++ { // 限制监控次数
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 尝试获取topic信息
			info, err := consumer.GetTopicInfo(ctx)
			if err != nil {
				t.Logf("❌ 获取topic信息时出错: %v", err)

				// 检查具体的错误类型
				if err.Error() == "获取Stream信息失败: ERR no such key" {
					t.Log("🔍 检测到: Stream已被删除")
				}
				continue
			}

			if !info.Exists {
				t.Log("🔍 检测到: Topic不存在")
				continue
			}

			t.Logf("📊 Topic状态: %d 条消息, %d 个消费者组", info.Length, len(info.Groups))

			// 尝试直接查询Redis来检查可能的错误
			checkRedisErrors(t, rdb, streamName, groupName)
		}
	}
}

// checkRedisErrors 直接检查Redis可能出现的错误
func checkRedisErrors(t *testing.T, rdb *redis.Client, streamName, groupName string) {
	ctx := context.Background()

	// 1. 尝试XREADGROUP - 这是消费者最常遇到的错误
	_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "test-consumer",
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    time.Millisecond * 100,
	}).Result()

	if err != nil && err != redis.Nil {
		t.Logf("❌ XREADGROUP错误: %v", err)

		// 分析错误类型
		switch {
		case err.Error() == "NOGROUP No such key '"+streamName+"' or consumer group '"+groupName+"' in XREADGROUP with GROUP option":
			t.Log("🔍 错误分析: 消费者组不存在")
		case err.Error() == "ERR no such key":
			t.Log("🔍 错误分析: Stream不存在")
		default:
			t.Logf("🔍 错误分析: 其他错误 - %s", err.Error())
		}
	}

	// 2. 尝试XACK - ACK操作的错误
	err = rdb.XAck(ctx, streamName, groupName, "test-message-id").Err()
	if err != nil {
		t.Logf("❌ XACK错误: %v", err)
	}

	// 3. 尝试XPENDING - 查询pending消息的错误
	_, err = rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()

	if err != nil {
		t.Logf("❌ XPENDING错误: %v", err)
	}

	// 4. 尝试XCLAIM - 抢夺消息的错误
	_, err = rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: "test-consumer",
		MinIdle:  0,
		Messages: []string{"test-message-id"},
	}).Result()

	if err != nil {
		t.Logf("❌ XCLAIM错误: %v", err)
	}
}

// TestSpecificErrorTypes 测试特定的错误类型
func TestSpecificErrorTypes(t *testing.T) {
	// 创建Redis客户端
	rdb := getRDB()

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("跳过测试：无法连接到Redis: %v", err)
		return
	}

	streamName := "test-error-types"
	groupName := "test-group"

	t.Log("🧪 测试特定错误类型")

	// 测试对不存在的Stream进行操作
	t.Log("📝 测试1: 对不存在的Stream进行XREADGROUP操作")
	_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "test-consumer",
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    time.Millisecond * 100,
	}).Result()

	if err != nil {
		t.Logf("✅ 预期的XREADGROUP错误: %v", err)
	}

	// 测试对不存在的Stream进行ACK操作
	t.Log("📝 测试2: 对不存在的Stream进行XACK操作")
	err = rdb.XAck(ctx, streamName, groupName, "fake-message-id").Err()
	if err != nil {
		t.Logf("✅ 预期的XACK错误: %v", err)
	}

	// 测试获取不存在Stream的信息
	t.Log("📝 测试3: 获取不存在Stream的信息")
	_, err = rdb.XInfoStream(ctx, streamName).Result()
	if err != nil {
		t.Logf("✅ 预期的XINFO错误: %v", err)
	}

	t.Log("✅ 错误类型测试完成")
}
