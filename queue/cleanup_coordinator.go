package queue

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

// CleanupCoordinator 清理协调器，用于协调多个消费者的清理操作
type CleanupCoordinator struct {
	client     *redis.Client
	streamName string
	lockKey    string
	lockTTL    time.Duration
}

// NewCleanupCoordinator 创建清理协调器
func NewCleanupCoordinator(client *redis.Client, streamName string) *CleanupCoordinator {
	return &CleanupCoordinator{
		client:     client,
		streamName: streamName,
		lockKey:    fmt.Sprintf("cleanup_lock:%s", streamName),
		lockTTL:    time.Minute * 2, // 锁的TTL为2分钟
	}
}

// TryAcquireCleanupLock 尝试获取清理锁
func (cc *CleanupCoordinator) TryAcquireCleanupLock(ctx context.Context, consumerName string) (bool, error) {
	// 使用Redis SET命令的NX选项实现分布式锁
	result, err := cc.client.SetNX(ctx, cc.lockKey, consumerName, cc.lockTTL).Result()
	if err != nil {
		return false, fmt.Errorf("获取清理锁失败: %w", err)
	}

	if result {
		log.Printf("消费者 %s 获取到清理锁", consumerName)
		return true, nil
	}

	// 检查锁的持有者
	holder, err := cc.client.Get(ctx, cc.lockKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("检查锁持有者失败: %v", err)
	} else if !errors.Is(err, redis.Nil) {
		log.Printf("清理锁被 %s 持有，跳过清理", holder)
	}

	return false, nil
}

// ReleaseCleanupLock 释放清理锁
func (cc *CleanupCoordinator) ReleaseCleanupLock(ctx context.Context, consumerName string) error {
	// 使用Lua脚本确保只有锁的持有者才能释放锁
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	result, err := cc.client.Eval(ctx, luaScript, []string{cc.lockKey}, consumerName).Result()
	if err != nil {
		return fmt.Errorf("释放清理锁失败: %w", err)
	}

	if result.(int64) == 1 {
		log.Printf("消费者 %s 释放清理锁", consumerName)
	} else {
		log.Printf("消费者 %s 无法释放清理锁（可能已过期或被其他消费者持有）", consumerName)
	}

	return nil
}

// ExtendCleanupLock 延长清理锁的TTL
func (cc *CleanupCoordinator) ExtendCleanupLock(ctx context.Context, consumerName string) error {
	// 使用Lua脚本确保只有锁的持有者才能延长TTL
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("EXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	result, err := cc.client.Eval(ctx, luaScript, []string{cc.lockKey}, consumerName, int(cc.lockTTL.Seconds())).Result()
	if err != nil {
		return fmt.Errorf("延长清理锁失败: %w", err)
	}

	if result.(int64) == 1 {
		log.Printf("消费者 %s 延长清理锁TTL", consumerName)
	}

	return nil
}

// IsCleanupInProgress 检查是否有清理操作正在进行
func (cc *CleanupCoordinator) IsCleanupInProgress(ctx context.Context) (bool, string, error) {
	holder, err := cc.client.Get(ctx, cc.lockKey).Result()
	if errors.Is(err, redis.Nil) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("检查清理状态失败: %w", err)
	}

	return true, holder, nil
}

// GetCleanupStats 获取清理统计信息
func (cc *CleanupCoordinator) GetCleanupStats(ctx context.Context) (*CleanupStats, error) {
	statsKey := fmt.Sprintf("cleanup_stats:%s", cc.streamName)

	stats, err := cc.client.HGetAll(ctx, statsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("获取清理统计失败: %w", err)
	}

	cleanupStats := &CleanupStats{
		StreamName: cc.streamName,
		Stats:      stats,
	}

	return cleanupStats, nil
}

// UpdateCleanupStats 更新清理统计信息
func (cc *CleanupCoordinator) UpdateCleanupStats(ctx context.Context, consumerName string, cleaned int) error {
	statsKey := fmt.Sprintf("cleanup_stats:%s", cc.streamName)
	now := time.Now().Format(time.RFC3339)

	pipe := cc.client.Pipeline()
	pipe.HIncrBy(ctx, statsKey, "total_cleaned", int64(cleaned))
	pipe.HIncrBy(ctx, statsKey, "cleanup_count", 1)
	pipe.HSet(ctx, statsKey, "last_cleanup_time", now)
	pipe.HSet(ctx, statsKey, "last_cleanup_by", consumerName)
	pipe.HSet(ctx, statsKey, "last_cleaned_count", cleaned)
	pipe.Expire(ctx, statsKey, time.Hour*24*7) // 统计信息保留7天

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("更新清理统计失败: %w", err)
	}

	log.Printf("更新清理统计: 消费者=%s, 清理数量=%d", consumerName, cleaned)
	return nil
}

// CleanupStats 清理统计信息
type CleanupStats struct {
	StreamName string            `json:"stream_name"`
	Stats      map[string]string `json:"stats"`
}

// CoordinatedCleanup 协调的清理操作
func (mq *MessageQueue) CoordinatedCleanup(ctx context.Context) (int, error) {
	coordinator := NewCleanupCoordinator(mq.client, mq.streamName)

	// 尝试获取清理锁
	acquired, err := coordinator.TryAcquireCleanupLock(ctx, mq.consumerName)
	if err != nil {
		return 0, fmt.Errorf("获取清理锁失败: %w", err)
	}

	if !acquired {
		log.Printf("未能获取清理锁，跳过清理操作")
		return 0, nil
	}

	// 确保释放锁
	defer func() {
		if err := coordinator.ReleaseCleanupLock(ctx, mq.consumerName); err != nil {
			log.Printf("释放清理锁失败: %v", err)
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
