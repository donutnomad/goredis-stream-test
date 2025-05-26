package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// CleanupStats 清理统计信息
type CleanupStats struct {
	StreamName string            `json:"stream_name"`
	Stats      map[string]string `json:"stats"`
}

type MessageCleaner struct {
	client        *redis.Client
	streamName    string
	consumerName  string
	cleanupPolicy *CleanupPolicy
	wg            sync.WaitGroup
	stopChan      chan struct{}
}

// CoordinatedCleanup 协调的清理操作
func (mq *MessageCleaner) CoordinatedCleanup(ctx context.Context) (int, error) {
	coordinator := NewCleanupCoordinator(mq.client, mq.streamName)

	// 尝试获取清理锁
	mutex, acquired, err := coordinator.TryAcquireCleanupLock(ctx, mq.consumerName)
	if err != nil {
		return 0, fmt.Errorf("获取清理锁失败: %w", err)
	}

	if !acquired {
		log.Printf("未能获取清理锁，跳过清理操作")
		return 0, nil
	}

	// 确保释放锁
	defer func() {
		if mutex != nil {
			if _, err := mutex.Unlock(); err != nil {
				log.Printf("释放清理锁失败: %v", err)
			}
		}
	}()

	// 执行清理操作
	cleaned, err := mq.CleanupMessages(ctx)
	if err != nil {
		return 0, fmt.Errorf("执行清理失败: %w", err)
	}

	// 更新统计信息
	if cleaned > 0 {
		if err := coordinator.UpdateCleanupStats(ctx, mq.consumerName, cleaned); err != nil {
			log.Printf("更新清理统计失败: %v", err)
		}
	}

	return cleaned, nil
}

// SetCleanupPolicy 设置清理策略
func (mq *MessageCleaner) SetCleanupPolicy(policy *CleanupPolicy) {
	mq.cleanupPolicy = policy
}

// GetCleanupPolicy 获取清理策略
func (mq *MessageQueue) GetCleanupPolicy() *CleanupPolicy {
	return mq.cleanupPolicy
}

// CleanupMessages 手动清理消息
func (mq *MessageCleaner) CleanupMessages(ctx context.Context) (int, error) {
	policy := mq.cleanupPolicy

	log.Printf("开始手动清理Stream %s", mq.streamName)

	// 查找可以清理的消息
	cleanableMessages, err := mq.findCleanableMessages(ctx, policy)
	if err != nil {
		return 0, fmt.Errorf("查找可清理消息失败: %w", err)
	}

	if len(cleanableMessages) == 0 {
		log.Printf("没有找到可清理的消息")
		return 0, nil
	}

	// 执行清理
	cleaned, err := mq.cleanMessages(ctx, cleanableMessages, policy.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("清理消息失败: %w", err)
	}

	log.Printf("手动清理完成，清理了 %d 条消息", cleaned)
	return cleaned, nil
}

// cleanMessages 清理指定的消息
func (mq *MessageCleaner) cleanMessages(ctx context.Context, messageIDs []string, batchSize int64) (int, error) {
	if len(messageIDs) == 0 {
		return 0, nil
	}

	cleaned := 0
	batchCount := int(batchSize)

	for i := 0; i < len(messageIDs); i += batchCount {
		end := i + batchCount
		if end > len(messageIDs) {
			end = len(messageIDs)
		}

		batch := messageIDs[i:end]

		// 使用XDEL删除消息
		deleted, err := mq.client.XDel(ctx, mq.streamName, batch...).Result()
		if err != nil {
			return cleaned, fmt.Errorf("删除消息批次失败: %w", err)
		}

		cleaned += int(deleted)
		log.Printf("删除了 %d 条消息，批次: %d-%d", deleted, i+1, end)

		// 避免一次性删除太多消息影响性能
		if i+batchCount < len(messageIDs) {
			time.Sleep(time.Millisecond * 100)
		}
	}

	return cleaned, nil
}

// findCleanableMessages 查找可以清理的消息
func (mq *MessageCleaner) findCleanableMessages(ctx context.Context, policy *CleanupPolicy) ([]string, error) {
	// 获取所有消费者组
	groups, err := mq.client.XInfoGroups(ctx, mq.streamName).Result()
	if err != nil {
		return nil, fmt.Errorf("获取消费者组失败: %w", err)
	}
	if len(groups) == 0 {
		log.Printf("没有消费者组，无法确定可清理的消息")
		return nil, nil
	}
	// 获取所有消费者组的最小已确认消息ID
	minDeliveredID := ""
	for i, group := range groups {
		if i == 0 || compareMessageIDs(group.LastDeliveredID, minDeliveredID) < 0 {
			minDeliveredID = group.LastDeliveredID
		}
	}
	if minDeliveredID == "" || minDeliveredID == "0-0" {
		log.Printf("没有找到有效的最小已确认消息ID")
		return nil, nil
	}
	// 获取可以清理的消息（在最小已确认ID之前的消息）
	cleanableMessages := make([]string, 0)
	// 使用XRANGE获取从开始到最小已确认ID之前的消息
	messages, err := mq.client.XRange(ctx, mq.streamName, "-", minDeliveredID).Result()
	if err != nil {
		return nil, fmt.Errorf("获取消息范围失败: %w", err)
	}
	// 检查每个消息是否被所有消费者组确认
	for _, msg := range messages {
		if mq.isMessageFullyAcked(ctx, msg.ID, groups) {
			// 检查消息年龄
			if isMessageOldEnough(msg.ID, policy.MinRetentionTime) {
				cleanableMessages = append(cleanableMessages, msg.ID)
			}
		}
	}
	return cleanableMessages, nil
}

// isMessageFullyAcked 检查消息是否被所有消费者组确认
func (mq *MessageCleaner) isMessageFullyAcked(ctx context.Context, messageID string, groups []redis.XInfoGroup) bool {
	for _, group := range groups {
		// 检查消息是否在该组的pending列表中
		pending, err := mq.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: mq.streamName,
			Group:  group.Name,
			Start:  messageID,
			End:    messageID,
			Count:  1,
		}).Result()

		if err != nil {
			log.Printf("检查pending消息失败 %s: %v", messageID, err)
			return false
		}

		// 如果消息在pending列表中，说明未被确认
		if len(pending) > 0 {
			return false
		}
	}

	return true
}

// autoCleanupMessages 自动清理已处理的消息
func (mq *MessageCleaner) autoCleanupMessages(ctx context.Context) {
	defer mq.wg.Done()

	log.Printf("启动自动清理goroutine，间隔: %v", mq.cleanupPolicy.CleanupInterval)

	ticker := time.NewTicker(mq.cleanupPolicy.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mq.stopChan:
			log.Printf("停止自动清理goroutine")
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			mq.performCleanup(ctx)
		}
	}
}

// performCleanup 执行清理操作
func (mq *MessageCleaner) performCleanup(ctx context.Context) {
	policy := mq.cleanupPolicy
	if !policy.EnableAutoCleanup {
		return
	}

	// 获取Stream信息
	streamInfo, err := mq.client.XInfoStream(ctx, mq.streamName).Result()
	if err != nil {
		if err.Error() != "ERR no such key" {
			log.Printf("获取Stream信息失败: %v", err)
		}
		return
	}

	// 检查是否需要清理
	if streamInfo.Length <= policy.MaxStreamLength {
		log.Printf("Stream长度 %d 未超过限制 %d，跳过清理", streamInfo.Length, policy.MaxStreamLength)
		return
	}

	log.Printf("开始协调清理Stream %s，当前长度: %d", mq.streamName, streamInfo.Length)

	// 使用协调器执行清理
	cleaned, err := mq.CoordinatedCleanup(ctx)
	if err != nil {
		log.Printf("协调清理失败: %v", err)
		return
	}

	if cleaned > 0 {
		log.Printf("协调清理成功，清理了 %d 条消息", cleaned)
	}
}
