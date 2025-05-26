package queue

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
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
	rs         *redsync.Redsync
}

// NewCleanupCoordinator 创建清理协调器
func NewCleanupCoordinator(client *redis.Client, streamName string) *CleanupCoordinator {
	// 初始化 redsync
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	return &CleanupCoordinator{
		client:     client,
		streamName: streamName,
		lockKey:    fmt.Sprintf("cleanup_lock:%s", streamName),
		lockTTL:    time.Minute * 2, // 锁的TTL为2分钟
		rs:         rs,
	}
}

// TryAcquireCleanupLock 尝试获取清理锁
func (cc *CleanupCoordinator) TryAcquireCleanupLock(ctx context.Context, consumerName string) (*AutoExtendMutex, bool, error) {
	// 使用 redsync 创建互斥锁
	mutex := cc.rs.NewMutex(cc.lockKey,
		redsync.WithExpiry(cc.lockTTL),
		redsync.WithTries(1), // 只尝试一次
		redsync.WithValue(consumerName),
	)

	// 创建自动续期的互斥锁
	autoMutex := NewAutoExtendMutex(mutex, cc.lockTTL/2)

	// 尝试获取锁
	err := autoMutex.LockContext(ctx)
	if err != nil {
		// 如果获取锁失败，检查锁的持有者
		holder, getErr := cc.client.Get(ctx, cc.lockKey).Result()
		if getErr != nil && !errors.Is(getErr, redis.Nil) {
			log.Printf("检查锁持有者失败: %v", getErr)
		} else if !errors.Is(getErr, redis.Nil) {
			log.Printf("清理锁被 %s 持有，跳过清理", holder)
		}

		return nil, false, fmt.Errorf("获取清理锁失败: %w", err)
	}

	log.Printf("消费者 %s 获取到清理锁", consumerName)
	return autoMutex, true, nil
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

// CleanupStats 清理统计信息
type CleanupStats struct {
	StreamName string            `json:"stream_name"`
	Stats      map[string]string `json:"stats"`
}

// CoordinatedCleanup 协调的清理操作
func (mq *MessageQueue) CoordinatedCleanup(ctx context.Context) (int, error) {
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
