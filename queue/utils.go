package queue

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// extractTimestampFromMessageID 从消息ID中提取时间戳
func extractTimestampFromMessageID(messageID string) (int64, error) {
	// Redis Stream消息ID格式: timestamp-sequence
	parts := strings.Split(messageID, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("无效的消息ID格式: %s", messageID)
	}

	timestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析时间戳失败: %w", err)
	}

	return timestamp, nil
}

// compareMessageIDs 比较两个消息ID
func compareMessageIDs(id1, id2 string) int {
	if id1 == id2 {
		return 0
	}

	ts1, err1 := extractTimestampFromMessageID(id1)
	ts2, err2 := extractTimestampFromMessageID(id2)

	if err1 != nil || err2 != nil {
		// 如果解析失败，使用字符串比较
		if id1 < id2 {
			return -1
		}
		return 1
	}

	if ts1 < ts2 {
		return -1
	} else if ts1 > ts2 {
		return 1
	}

	// 时间戳相同，比较序列号
	parts1 := strings.Split(id1, "-")
	parts2 := strings.Split(id2, "-")

	if len(parts1) == 2 && len(parts2) == 2 {
		seq1, _ := strconv.ParseInt(parts1[1], 10, 64)
		seq2, _ := strconv.ParseInt(parts2[1], 10, 64)

		if seq1 < seq2 {
			return -1
		} else if seq1 > seq2 {
			return 1
		}
	}

	return 0
}

// isMessageOldEnough 检查消息是否足够老可以被清理
func isMessageOldEnough(messageID string, minRetentionTime time.Duration) bool {
	// 从消息ID中提取时间戳
	timestamp, err := extractTimestampFromMessageID(messageID)
	if err != nil {
		log.Printf("提取消息时间戳失败 %s: %v", messageID, err)
		return false
	}
	return time.Since(MillisToTime(timestamp)) >= minRetentionTime
}

func MillisToTime(millis int64) time.Time {
	return time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond))
}

// GetTopicInfo 获取topic信息（消息数量、消费者组等）
func getTopicInfo(ctx context.Context, client *redis.Client, streamName string) (*TopicInfo, error) {
	info := &TopicInfo{
		StreamName: streamName,
	}

	// 获取Stream信息
	streamInfo, err := client.XInfoStream(ctx, streamName).Result()
	if err != nil {
		if err.Error() == "ERR no such key" {
			info.Exists = false
			return info, nil
		}
		return nil, fmt.Errorf("获取Stream信息失败: %w", err)
	}

	info.Exists = true
	info.Length = streamInfo.Length
	info.FirstEntryID = streamInfo.FirstEntry.ID
	info.LastEntryID = streamInfo.LastEntry.ID

	// 获取消费者组信息
	groups, err := client.XInfoGroups(ctx, streamName).Result()
	if err != nil {
		return nil, fmt.Errorf("获取消费者组信息失败: %w", err)
	}

	info.Groups = make([]GroupInfo, len(groups))
	for i, group := range groups {
		info.Groups[i] = GroupInfo{
			Name:            group.Name,
			Pending:         group.Pending,
			LastDeliveredID: group.LastDeliveredID,
		}

		// 获取消费者信息
		consumers, err := client.XInfoConsumers(ctx, streamName, group.Name).Result()
		if err != nil {
			log.Printf("获取消费者组 %s 的消费者信息失败: %v", group.Name, err)
			continue
		}

		info.Groups[i].Consumers = make([]ConsumerInfo, len(consumers))
		for j, consumer := range consumers {
			info.Groups[i].Consumers[j] = ConsumerInfo{
				Name:    consumer.Name,
				Pending: consumer.Pending,
				Idle:    consumer.Idle,
			}
		}
	}

	return info, nil
}

func excludeID(id string) string {
	return "(" + id // 使用 ( 前缀表示不包含该ID，即从该ID之后的消息开始
}

type Slices[T any] []T

func (s Slices[T]) Last() T {
	return s[len(s)-1]
}

// isStreamOrGroupDeletedError 检查错误是否是因为Stream或消费者组被删除
func isStreamOrGroupDeletedError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// NOGROUP No such key 'error-demo-test' or consumer group 'error-group-test' in XREADGROUP with GROUP option
	// 常见的错误模式
	patterns := []string{
		"ERR no such key",
		"NOGROUP",
		"No such key",
		"consumer group",
		"does not exist",
	}

	for _, pattern := range patterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

func extractIds(messages []*Message) []string {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
	}
	return ids
}
