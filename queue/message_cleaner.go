package queue

import (
	"context"
	"fmt"
	"iter"
	"log"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
)

const SMALL_POS = "-" // 初始从最小ID开始
const BIG_POS = "+"

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

func NewMessageCleaner(client *redis.Client, streamName, consumerName string) *MessageCleaner {
	return &MessageCleaner{
		client:        client,
		streamName:    streamName,
		consumerName:  consumerName,
		cleanupPolicy: DefaultCleanupPolicy(),
		wg:            sync.WaitGroup{},
		stopChan:      make(chan struct{}),
	}
}

// CoordinatedCleanup 协调的清理操作
func (mq *MessageCleaner) CoordinatedCleanup(ctx context.Context) (int64, error) {
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

func (mq *MessageCleaner) SetCleanupPolicy(policy *CleanupPolicy) {
	mq.cleanupPolicy = policy
}

func (mq *MessageCleaner) GetCleanupPolicy() *CleanupPolicy {
	return mq.cleanupPolicy
}

// CleanupMessages 手动清理消息
func (mq *MessageCleaner) CleanupMessages(ctx context.Context) (totalDeleted int64, _ error) {
	policy := mq.cleanupPolicy
	log.Printf("开始手动清理Stream %s", mq.streamName)
	for msgIds, err := range mq.findCleanableMessages(ctx, policy, int64(200)) {
		if err != nil {
			return 0, err
		}
		deleted, err := mq.client.XDel(ctx, mq.streamName, msgIds...).Result()
		if err != nil {
			log.Printf("删除消息批次失败: %v", err)
			continue
		}
		totalDeleted += deleted
	}
	log.Printf("手动清理完成，清理了 %d 条消息", totalDeleted)
	return totalDeleted, nil
}

func (mq *MessageCleaner) getMinDeliveredIDForAllGroup(ctx context.Context) ([]string, *string, error) {
	groups, err := mq.client.XInfoGroups(ctx, mq.streamName).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("[cleaner] call XInfoGroups failed: %w", err)
	}
	if len(groups) == 0 {
		return nil, nil, nil
	}
	minDeliveredID := ""
	for i, group := range groups {
		if i == 0 || compareMessageIDs(group.LastDeliveredID, minDeliveredID) < 0 {
			minDeliveredID = group.LastDeliveredID
		}
	}
	if minDeliveredID == "" || minDeliveredID == "0-0" {
		return nil, nil, nil
	}
	return lo.Map(groups, func(item redis.XInfoGroup, index int) string {
		return item.Name
	}), &minDeliveredID, nil
}

func (mq *MessageCleaner) findCleanableMessages(ctx context.Context, policy *CleanupPolicy, batchSize int64) iter.Seq2[[]string, error] {
	var startID = SMALL_POS
	return func(yield func([]string, error) bool) {
		groupNames, minDeliveredID, err := mq.getMinDeliveredIDForAllGroup(ctx)
		if err != nil || minDeliveredID == nil {
			yield(nil, err)
			return
		}
		for {
			messages, err := mq.client.XRangeN(ctx, mq.streamName, startID, *minDeliveredID, batchSize).Result()
			if err != nil {
				yield(nil, fmt.Errorf("[cleaner] XRangeN failed: %w", err))
				return
			}
			if len(messages) == 0 {
				break
			}
			startID = excludeID(messages[len(messages)-1].ID)
			candidateMessages := lo.Filter(messages, func(msg redis.XMessage, index int) bool {
				return isMessageOldEnough(msg.ID, policy.MinRetentionTime)
			})
			candidateIDs := lo.Map(candidateMessages, func(msg redis.XMessage, _ int) string {
				return msg.ID
			})
			ids := slices.Collect(mq.areMessagesFullyAcked(ctx, candidateIDs, groupNames))
			if !yield(ids, nil) {
				return
			}
			if int64(len(messages)) < batchSize {
				break
			}
		}

	}
}

// areMessagesFullyAcked 批量检查多个消息是否被所有消费者组确认
func (mq *MessageCleaner) areMessagesFullyAcked(ctx context.Context, messageIDs []string, groupNames []string) iter.Seq[string] {
	if len(messageIDs) == 0 {
		return func(yield func(string) bool) {}
	}
	ackIds := make(map[string]bool)
	for _, id := range messageIDs {
		ackIds[id] = true // 默认为已确认
	}
	for _, groupName := range groupNames {
		// 先检查该消费者组是否有pending消息
		pendingSummary, err := mq.client.XPending(ctx, mq.streamName, groupName).Result()
		if err != nil {
			log.Printf("获取消费者组pending摘要失败 %s: %v", groupName, err)
			// 出错时保守处理，假设所有消息未确认
			for id := range ackIds {
				ackIds[id] = false
			}
			continue
		}
		// 如果该组没有pending消息，直接跳过检查
		if pendingSummary.Count == 0 {
			log.Printf("消费者组 %s 没有pending消息，跳过检查", groupName)
			continue
		}

		// 对消息ID进行排序，确保顺序查询
		sortedIDs := make([]string, len(messageIDs))
		copy(sortedIDs, messageIDs)
		sort.Strings(sortedIDs)

		for start := 0; start < len(sortedIDs); start += 100 {
			end := min(start+100, len(sortedIDs))
			if len(sortedIDs[start:end]) == 0 {
				continue
			}
			startID := sortedIDs[start]
			endID := sortedIDs[end-1]
			pendingMessages, err := mq.client.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: mq.streamName,
				Group:  groupName,
				Start:  startID,
				End:    endID,
				Count:  int64(end - start),
			}).Result()
			if err != nil {
				log.Printf("批量检查pending消息失败 %s-%s: %v", startID, endID, err)
				// 出错时保守处理，将所有消息标记为未确认
				for i := start; i < end; i++ {
					ackIds[sortedIDs[i]] = false
				}
				continue
			}
			// 将找到的pending消息标记为未确认
			for _, pending := range pendingMessages {
				ackIds[pending.ID] = false
			}
		}
	}

	return func(yield func(string) bool) {
		for k, ack := range ackIds {
			if ack {
				if !yield(k) {
					return
				}
			}
		}
	}
}

func (mq *MessageCleaner) Stop() {
	close(mq.stopChan)
}

func (mq *MessageCleaner) Wait() {
	mq.wg.Wait()
}

func (mq *MessageCleaner) OnStart(ctx context.Context) {
	if mq.cleanupPolicy.EnableAutoCleanup {
		mq.wg.Add(1)
		go mq.autoCleanupMessages(ctx)
		log.Printf("已启用自动清理，间隔: %v", mq.cleanupPolicy.CleanupInterval)
	}
}

// autoCleanupMessages 自动清理已处理的消息
func (mq *MessageCleaner) autoCleanupMessages(ctx context.Context) {
	defer mq.wg.Done()
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
