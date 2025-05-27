package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Producer 消息生产者
type Producer struct {
	client     *redis.Client
	streamName string
	logger     ILogger
}

// NewProducer 创建新的消息生产者
func NewProducer(client *redis.Client, streamName string) *Producer {
	// 创建基础logger
	baseLogger := &defaultLogger{}

	// 使用包装器包装logger
	wrappedLogger := newLoggerWrapper(baseLogger, "[mq]")

	return &Producer{
		client:     client,
		streamName: streamName,
		logger:     wrappedLogger,
	}
}

// WithLogger 为Producer设置自定义日志器
func (p *Producer) WithLogger(logger ILogger) *Producer {
	// 使用包装器包装传入的logger
	p.logger = newLoggerWrapper(logger, "[mq]")
	return p
}

func (mq *Producer) PublishMessage(ctx context.Context, msgType string, data map[string]any, metadata map[string]string) (string, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("序列化data失败: %w", err)
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("序列化metadata失败: %w", err)
	}

	messageID, err := mq.client.XAdd(ctx, &redis.XAddArgs{
		Stream: mq.streamName,
		Values: map[string]any{
			"type":     msgType,
			"data":     string(dataBytes),
			"metadata": string(metadataBytes),
		},
	}).Result()

	if err != nil {
		return "", fmt.Errorf("发布消息失败: %w", err)
	}

	mq.logger.Printf("消息已发布: %s, 类型: %s", messageID, msgType)
	return messageID, nil
}

// GetTopicInfo 获取topic信息（消息数量、消费者组等）
func (mq *Producer) GetTopicInfo(ctx context.Context) (*TopicInfo, error) {
	return getTopicInfo(ctx, mq.client, mq.streamName)
}
