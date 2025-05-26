package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Message 消息结构
type Message struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Data     map[string]interface{} `json:"data"`
	Metadata map[string]string      `json:"metadata"`
}

// MessageHandler 消息处理器接口
type MessageHandler interface {
	Handle(ctx context.Context, msg *Message) error
	GetMessageType() string
}

// BatchMessageHandler 批量消息处理器接口
type BatchMessageHandler interface {
	HandleBatch(ctx context.Context, messages []*Message) error
	GetMessageType() string
	// GetBatchSize 返回期望的批量大小，返回0表示使用全局配置
	GetBatchSize() int
}

// StartPosition 消费者组开始位置
type StartPosition string

const (
	// StartFromLatest 从最新消息开始消费（默认）
	StartFromLatest StartPosition = "$"
	// StartFromEarliest 从最早消息开始消费
	StartFromEarliest StartPosition = "0"
	// StartFromSpecific 从指定消息ID开始消费
	StartFromSpecific StartPosition = "specific"
)

// CleanupPolicy 清理策略
type CleanupPolicy struct {
	// EnableAutoCleanup 是否启用自动清理
	EnableAutoCleanup bool
	// CleanupInterval 清理间隔
	CleanupInterval time.Duration
	// MaxStreamLength 最大Stream长度，超过时触发清理
	MaxStreamLength int64
	// MinRetentionTime 最小保留时间，消息必须存在这么长时间才能被清理
	MinRetentionTime time.Duration
	// BatchSize 每次清理的批次大小
	BatchSize int64
}

// DefaultCleanupPolicy 默认清理策略
func DefaultCleanupPolicy() *CleanupPolicy {
	return &CleanupPolicy{
		EnableAutoCleanup: false, // 默认不启用
		CleanupInterval:   time.Minute * 5,
		MaxStreamLength:   10000,
		MinRetentionTime:  time.Hour * 1,
		BatchSize:         100,
	}
}

// BatchConfig 批量处理配置
type BatchConfig struct {
	// EnableBatch 是否启用批量处理
	EnableBatch bool
	// BatchSize 批量大小，默认为10
	BatchSize int
	// BatchTimeout 批量等待超时时间，如果在此时间内没有收集到足够的消息，则处理已有的消息
	BatchTimeout time.Duration
	// MaxWaitTime 最大等待时间，超过此时间强制处理已收集的消息
	MaxWaitTime time.Duration
}

// DefaultBatchConfig 默认批量配置
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		EnableBatch:  false,                  // 默认不启用批量处理
		BatchSize:    10,                     // 默认批量大小
		BatchTimeout: time.Millisecond * 500, // 500ms超时
		MaxWaitTime:  time.Second * 2,        // 最大等待2秒
	}
}

// MessageQueue Redis Stream消息队列
type MessageQueue struct {
	client       *redis.Client
	streamName   string
	groupName    string
	consumerName string
	startPos     StartPosition
	specificID   string // 当startPos为StartFromSpecific时使用
	handlers     map[string]MessageHandler
	stopChan     chan struct{}
	wg           sync.WaitGroup
	mu           sync.RWMutex
	stopped      bool

	// 清理策略
	cleanupPolicy *CleanupPolicy

	// 批量处理相关
	batchHandlers map[string]BatchMessageHandler
	batchConfig   *BatchConfig

	// 自动停止相关
	doneChan     chan struct{} // 用于通知外部消费者已停止
	autoStopped  bool          // 标记是否因为topic删除而自动停止
	autoStopOnce sync.Once     // 确保只自动停止一次
}

type Producer struct {
	client     *redis.Client
	streamName string
}

func NewProducer(client *redis.Client, streamName string) *Producer {
	return &Producer{client, streamName}
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

	log.Printf("消息已发布: %s, 类型: %s", messageID, msgType)
	return messageID, nil
}

// GetTopicInfo 获取topic信息（消息数量、消费者组等）
func (mq *Producer) GetTopicInfo(ctx context.Context) (*TopicInfo, error) {
	return getTopicInfo(ctx, mq.client, mq.streamName)
}

// NewMessageQueue 创建新的消息队列（默认从最新位置开始）
func NewMessageQueue(client *redis.Client, streamName, groupName, consumerName string) *MessageQueue {
	return &MessageQueue{
		client:        client,
		streamName:    streamName,
		groupName:     groupName,
		consumerName:  consumerName,
		startPos:      StartFromLatest, // 默认从最新位置开始
		handlers:      make(map[string]MessageHandler),
		stopChan:      make(chan struct{}),
		cleanupPolicy: DefaultCleanupPolicy(),
		batchHandlers: make(map[string]BatchMessageHandler),
		batchConfig:   DefaultBatchConfig(),
		doneChan:      make(chan struct{}),
	}
}

// NewMessageQueueWithStartPosition 创建新的消息队列并指定开始位置
func NewMessageQueueWithStartPosition(client *redis.Client, streamName, groupName, consumerName string, startPos StartPosition) *MessageQueue {
	return &MessageQueue{
		client:        client,
		streamName:    streamName,
		groupName:     groupName,
		consumerName:  consumerName,
		startPos:      startPos,
		handlers:      make(map[string]MessageHandler),
		stopChan:      make(chan struct{}),
		cleanupPolicy: DefaultCleanupPolicy(),
		batchHandlers: make(map[string]BatchMessageHandler),
		batchConfig:   DefaultBatchConfig(),
		doneChan:      make(chan struct{}),
	}
}

// NewMessageQueueFromSpecificID 创建新的消息队列并从指定消息ID开始
func NewMessageQueueFromSpecificID(client *redis.Client, streamName, groupName, consumerName string, messageID string) *MessageQueue {
	return &MessageQueue{
		client:        client,
		streamName:    streamName,
		groupName:     groupName,
		consumerName:  consumerName,
		startPos:      StartFromSpecific,
		specificID:    messageID,
		handlers:      make(map[string]MessageHandler),
		stopChan:      make(chan struct{}),
		cleanupPolicy: DefaultCleanupPolicy(),
		batchHandlers: make(map[string]BatchMessageHandler),
		batchConfig:   DefaultBatchConfig(),
		doneChan:      make(chan struct{}),
	}
}

// RegisterHandler 注册消息处理器
func (mq *MessageQueue) RegisterHandler(handler MessageHandler) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.handlers[handler.GetMessageType()] = handler
}

// RegisterBatchHandler 注册批量消息处理器
func (mq *MessageQueue) RegisterBatchHandler(handler BatchMessageHandler) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.batchHandlers[handler.GetMessageType()] = handler
}

// GetStreamName 获取Stream名称
func (mq *MessageQueue) GetStreamName() string {
	return mq.streamName
}

// SetCleanupPolicy 设置清理策略
func (mq *MessageQueue) SetCleanupPolicy(policy *CleanupPolicy) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.cleanupPolicy = policy
}

// GetCleanupPolicy 获取清理策略
func (mq *MessageQueue) GetCleanupPolicy() *CleanupPolicy {
	mq.mu.RLock()
	defer mq.mu.RUnlock()
	return mq.cleanupPolicy
}

// SetBatchConfig 设置批量处理配置
func (mq *MessageQueue) SetBatchConfig(config *BatchConfig) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.batchConfig = config
}

// GetBatchConfig 获取批量处理配置
func (mq *MessageQueue) GetBatchConfig() *BatchConfig {
	mq.mu.RLock()
	defer mq.mu.RUnlock()
	return mq.batchConfig
}

// Done 返回一个通道，当消费者因为topic删除而自动停止时会关闭
func (mq *MessageQueue) Done() <-chan struct{} {
	return mq.doneChan
}

// IsAutoStopped 检查消费者是否因为topic删除而自动停止
func (mq *MessageQueue) IsAutoStopped() bool {
	mq.mu.RLock()
	defer mq.mu.RUnlock()
	return mq.autoStopped
}

// autoStop 自动停止消费者（因为topic被删除）
func (mq *MessageQueue) autoStop(reason string) {
	mq.autoStopOnce.Do(func() {
		mq.mu.Lock()
		if mq.stopped {
			mq.mu.Unlock()
			return
		}
		mq.autoStopped = true
		mq.stopped = true
		mq.mu.Unlock()

		log.Printf("消费者 %s 自动停止: %s", mq.consumerName, reason)

		// 关闭stopChan以停止所有goroutine
		close(mq.stopChan)

		// 关闭doneChan通知外部
		close(mq.doneChan)

		// 等待所有goroutine结束
		mq.wg.Wait()

		log.Printf("消费者 %s 已完全停止", mq.consumerName)
	})
}

// Start 启动消息队列
func (mq *MessageQueue) Start(ctx context.Context) error {
	mq.mu.Lock()
	if mq.stopped {
		mq.mu.Unlock()
		return fmt.Errorf("消息队列已停止")
	}
	mq.mu.Unlock()

	// 创建消费者组（如果不存在）
	err := mq.createConsumerGroup(ctx)
	if err != nil {
		return fmt.Errorf("创建消费者组失败: %w", err)
	}

	// 启动处理pending消息的goroutine
	mq.wg.Add(1)
	go mq.processPendingMessages(ctx)

	// 启动处理新消息的goroutine
	mq.wg.Add(1)
	go mq.processNewMessages(ctx)

	// 启动自动清理goroutine（如果启用）
	if mq.cleanupPolicy.EnableAutoCleanup {
		mq.wg.Add(1)
		go mq.autoCleanupMessages(ctx)
		log.Printf("已启用自动清理，间隔: %v", mq.cleanupPolicy.CleanupInterval)
	}

	log.Printf("消息队列已启动 - Stream: %s, Group: %s, Consumer: %s",
		mq.streamName, mq.groupName, mq.consumerName)

	return nil
}

// Stop 停止消息队列
func (mq *MessageQueue) Stop() {
	mq.mu.Lock()
	if mq.stopped {
		mq.mu.Unlock()
		return
	}
	mq.stopped = true
	mq.mu.Unlock()

	close(mq.stopChan)

	// 如果不是自动停止，则关闭doneChan
	if !mq.autoStopped {
		close(mq.doneChan)
	}

	mq.wg.Wait()
	log.Printf("消息队列已停止")
}

// createConsumerGroup 创建消费者组
func (mq *MessageQueue) createConsumerGroup(ctx context.Context) error {
	// 确定开始位置
	var startID string
	switch mq.startPos {
	case StartFromLatest:
		startID = "$"
	case StartFromEarliest:
		startID = "0"
	case StartFromSpecific:
		if mq.specificID == "" {
			return fmt.Errorf("使用StartFromSpecific时必须指定specificID")
		}
		startID = mq.specificID
	default:
		startID = "$" // 默认从最新开始
	}

	log.Printf("创建消费者组 %s，开始位置: %s", mq.groupName, startID)

	// 尝试创建消费者组
	err := mq.client.XGroupCreate(ctx, mq.streamName, mq.groupName, startID).Err()
	if err != nil && !strings.Contains(err.Error(), "Consumer Group name already exists") {
		// 如果stream不存在，先创建一个空的stream
		if strings.Contains(err.Error(), "ERR The XGROUP subcommand requires the key to exist") {
			_, err = mq.client.XAdd(ctx, &redis.XAddArgs{
				Stream: mq.streamName,
				Values: map[string]any{"init": "true"},
			}).Result()
			if err != nil {
				return err
			}
			// 再次尝试创建消费者组
			err = mq.client.XGroupCreate(ctx, mq.streamName, mq.groupName, startID).Err()
			if err != nil && !strings.Contains(err.Error(), "Consumer Group name already exists") {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

// processPendingMessages 处理pending列表中的消息
func (mq *MessageQueue) processPendingMessages(ctx context.Context) {
	defer mq.wg.Done()

	log.Printf("开始处理pending消息...")

	for {
		select {
		case <-mq.stopChan:
			return
		case <-ctx.Done():
			return
		default:
			// 获取pending消息
			pending, err := mq.client.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: mq.streamName,
				Group:  mq.groupName,
				Start:  "-",
				End:    "+",
				Count:  10,
			}).Result()

			if err != nil {
				// 检查是否是因为Stream或消费者组被删除
				if isStreamOrGroupDeletedError(err) {
					log.Printf("检测到Stream或消费者组已被删除，停止pending消息处理")
					mq.autoStop("Stream或消费者组被删除")
					return
				}

				time.Sleep(time.Second)
				continue
			}

			if len(pending) == 0 {
				log.Printf("没有pending消息，开始处理新消息")
				return // 没有pending消息，退出
			}

			// 处理每个pending消息
			for _, p := range pending {
				select {
				case <-mq.stopChan:
					return
				case <-ctx.Done():
					return
				default:
					mq.processPendingMessage(ctx, p.ID)
				}
			}
		}
	}
}

// processPendingMessage 处理单个pending消息
func (mq *MessageQueue) processPendingMessage(ctx context.Context, messageID string) {
	// 声明消息所有权
	claimed, err := mq.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   mq.streamName,
		Group:    mq.groupName,
		Consumer: mq.consumerName,
		MinIdle:  0,
		Messages: []string{messageID},
	}).Result()

	if err != nil {
		log.Printf("声明消息所有权失败 %s: %v", messageID, err)
		return
	}

	if len(claimed) == 0 {
		return
	}

	// 处理消息
	for _, msg := range claimed {
		mq.handleMessage(ctx, msg)
	}
}

// processNewMessages 处理新消息
func (mq *MessageQueue) processNewMessages(ctx context.Context) {
	defer mq.wg.Done()

	log.Printf("开始处理新消息...")

	// 检查是否启用批量处理
	batchConfig := mq.GetBatchConfig()
	if batchConfig.EnableBatch {
		mq.processNewMessagesBatch(ctx)
	} else {
		mq.processNewMessagesSingle(ctx)
	}
}

// processNewMessagesSingle 单条消息处理模式
func (mq *MessageQueue) processNewMessagesSingle(ctx context.Context) {
	for {
		select {
		case <-mq.stopChan:
			return
		case <-ctx.Done():
			return
		default:
			// 读取新消息
			streams, err := mq.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    mq.groupName,
				Consumer: mq.consumerName,
				Streams:  []string{mq.streamName, ">"},
				Count:    1,
				Block:    time.Second * 5,
			}).Result()

			if err != nil {
				if !errors.Is(err, redis.Nil) {
					log.Printf("读取新消息失败: %v", err)

					// 检查是否是因为Stream或消费者组被删除
					if isStreamOrGroupDeletedError(err) {
						log.Printf("检测到Stream或消费者组已被删除，停止消费者")
						mq.autoStop("Stream或消费者组被删除")
						return
					}
				}
				continue
			}

			// 处理消息
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					mq.handleMessage(ctx, msg)
				}
			}
		}
	}
}

// processNewMessagesBatch 批量消息处理模式
func (mq *MessageQueue) processNewMessagesBatch(ctx context.Context) {
	batchConfig := mq.GetBatchConfig()

	for {
		select {
		case <-mq.stopChan:
			return
		case <-ctx.Done():
			return
		default:
			// 读取批量消息
			streams, err := mq.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    mq.groupName,
				Consumer: mq.consumerName,
				Streams:  []string{mq.streamName, ">"},
				Count:    int64(batchConfig.BatchSize),
				Block:    batchConfig.BatchTimeout,
			}).Result()

			if err != nil {
				if !errors.Is(err, redis.Nil) {
					log.Printf("读取批量消息失败: %v", err)

					// 检查是否是因为Stream或消费者组被删除
					if isStreamOrGroupDeletedError(err) {
						log.Printf("检测到Stream或消费者组已被删除，停止消费者")
						mq.autoStop("Stream或消费者组被删除")
						return
					}
				}
				continue
			}

			// 处理批量消息
			for _, stream := range streams {
				if len(stream.Messages) > 0 {
					mq.handleMessagesBatch(ctx, stream.Messages)
				}
			}
		}
	}
}

// handleMessage 处理消息
func (mq *MessageQueue) handleMessage(ctx context.Context, msg redis.XMessage) {
	// 解析消息
	message, err := mq.parseMessage(msg)
	if err != nil {
		log.Printf("解析消息失败 %s: %v", msg.ID, err)
		mq.ackMessage(ctx, msg.ID)
		return
	}

	// 获取处理器
	mq.mu.RLock()
	handler, exists := mq.handlers[message.Type]
	mq.mu.RUnlock()

	if !exists {
		log.Printf("未找到消息类型 %s 的处理器", message.Type)
		mq.ackMessage(ctx, msg.ID)
		return
	}

	// 处理消息
	log.Printf("处理消息: %s, 类型: %s", msg.ID, message.Type)
	err = handler.Handle(ctx, message)
	if err != nil {
		log.Printf("处理消息失败 %s: %v", msg.ID, err)
		// 这里可以实现重试逻辑或者死信队列
		return
	}

	// ACK消息
	mq.ackMessage(ctx, msg.ID)
	log.Printf("消息处理完成: %s", msg.ID)
}

// handleMessagesBatch 批量处理消息
func (mq *MessageQueue) handleMessagesBatch(ctx context.Context, msgs []redis.XMessage) {
	if len(msgs) == 0 {
		return
	}

	// 按消息类型分组
	messageGroups := make(map[string][]*Message)
	messageIDGroups := make(map[string][]string)

	for _, msg := range msgs {
		// 解析消息
		message, err := mq.parseMessage(msg)
		if err != nil {
			log.Printf("解析消息失败 %s: %v", msg.ID, err)
			mq.ackMessage(ctx, msg.ID)
			continue
		}

		// 按类型分组
		messageGroups[message.Type] = append(messageGroups[message.Type], message)
		messageIDGroups[message.Type] = append(messageIDGroups[message.Type], msg.ID)
	}

	// 处理每个类型的消息组
	for msgType, messages := range messageGroups {
		messageIDs := messageIDGroups[msgType]

		// 获取批量处理器
		mq.mu.RLock()
		batchHandler, hasBatchHandler := mq.batchHandlers[msgType]
		singleHandler, hasSingleHandler := mq.handlers[msgType]
		mq.mu.RUnlock()

		if hasBatchHandler {
			// 使用批量处理器
			batchSize := batchHandler.GetBatchSize()
			if batchSize <= 0 {
				batchSize = mq.GetBatchConfig().BatchSize
			}

			// 如果消息数量超过批量大小，分批处理
			for i := 0; i < len(messages); i += batchSize {
				end := i + batchSize
				if end > len(messages) {
					end = len(messages)
				}

				batch := messages[i:end]
				batchIDs := messageIDs[i:end]

				log.Printf("批量处理消息: 类型=%s, 数量=%d", msgType, len(batch))
				err := batchHandler.HandleBatch(ctx, batch)
				if err != nil {
					log.Printf("批量处理消息失败 类型=%s: %v", msgType, err)
					// 这里可以实现重试逻辑或者死信队列
					continue
				}

				// 批量ACK消息
				for _, messageID := range batchIDs {
					mq.ackMessage(ctx, messageID)
				}
				log.Printf("批量消息处理完成: 类型=%s, 数量=%d", msgType, len(batch))
			}
		} else if hasSingleHandler {
			// 回退到单条处理
			log.Printf("未找到批量处理器，回退到单条处理: 类型=%s, 数量=%d", msgType, len(messages))
			for i, message := range messages {
				messageID := messageIDs[i]

				log.Printf("处理消息: %s, 类型: %s", messageID, msgType)
				err := singleHandler.Handle(ctx, message)
				if err != nil {
					log.Printf("处理消息失败 %s: %v", messageID, err)
					// 这里可以实现重试逻辑或者死信队列
					continue
				}

				// ACK消息
				mq.ackMessage(ctx, messageID)
				log.Printf("消息处理完成: %s", messageID)
			}
		} else {
			// 没有找到处理器
			log.Printf("未找到消息类型 %s 的处理器", msgType)
			for _, messageID := range messageIDs {
				mq.ackMessage(ctx, messageID)
			}
		}
	}
}

// parseMessage 解析消息
func (mq *MessageQueue) parseMessage(msg redis.XMessage) (*Message, error) {
	message := &Message{
		ID:       msg.ID,
		Data:     make(map[string]interface{}),
		Metadata: make(map[string]string),
	}

	// 解析消息字段
	if msgType, ok := msg.Values["type"].(string); ok {
		message.Type = msgType
	} else {
		return nil, fmt.Errorf("消息缺少type字段")
	}

	if dataStr, ok := msg.Values["data"].(string); ok {
		err := json.Unmarshal([]byte(dataStr), &message.Data)
		if err != nil {
			return nil, fmt.Errorf("解析data字段失败: %w", err)
		}
	}

	if metadataStr, ok := msg.Values["metadata"].(string); ok {
		err := json.Unmarshal([]byte(metadataStr), &message.Metadata)
		if err != nil {
			return nil, fmt.Errorf("解析metadata字段失败: %w", err)
		}
	}

	return message, nil
}

// ackMessage ACK消息
func (mq *MessageQueue) ackMessage(ctx context.Context, messageID string) {
	err := mq.client.XAck(ctx, mq.streamName, mq.groupName, messageID).Err()
	if err != nil {
		log.Printf("ACK消息失败 %s: %v", messageID, err)

		// 检查是否是因为Stream或消费者组被删除
		if isStreamOrGroupDeletedError(err) {
			log.Printf("检测到Stream或消费者组已被删除，ACK操作失败")
			mq.autoStop("Stream或消费者组被删除")
		}
	}
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

// TerminateTopic 终止整个topic，清空所有消息和消费者组
func (mq *MessageQueue) TerminateTopic(ctx context.Context) error {
	log.Printf("开始终止topic: %s", mq.streamName)

	// 首先停止当前消息队列的处理
	mq.Stop()

	// 获取所有消费者组信息
	groups, err := mq.client.XInfoGroups(ctx, mq.streamName).Result()
	if err != nil && err.Error() != "ERR no such key" {
		log.Printf("获取消费者组信息失败: %v", err)
	} else {
		// 删除所有消费者组
		for _, group := range groups {
			err = mq.client.XGroupDestroy(ctx, mq.streamName, group.Name).Err()
			if err != nil {
				log.Printf("删除消费者组 %s 失败: %v", group.Name, err)
			} else {
				log.Printf("已删除消费者组: %s", group.Name)
			}
		}
	}

	// 删除整个Stream（这会清空所有消息）
	deleted, err := mq.client.Del(ctx, mq.streamName).Result()
	if err != nil {
		return fmt.Errorf("删除Stream失败: %w", err)
	}

	if deleted > 0 {
		log.Printf("已删除Stream: %s，清空了所有消息", mq.streamName)
	} else {
		log.Printf("Stream %s 不存在或已为空", mq.streamName)
	}

	log.Printf("Topic %s 已完全终止", mq.streamName)
	return nil
}

// GetTopicInfo 获取topic信息（消息数量、消费者组等）
func (mq *MessageQueue) GetTopicInfo(ctx context.Context) (*TopicInfo, error) {
	return getTopicInfo(ctx, mq.client, mq.streamName)
}

// TopicInfo topic信息结构
type TopicInfo struct {
	StreamName   string      `json:"stream_name"`
	Exists       bool        `json:"exists"`
	Length       int64       `json:"length"`
	FirstEntryID string      `json:"first_entry_id"`
	LastEntryID  string      `json:"last_entry_id"`
	Groups       []GroupInfo `json:"groups"`
}

// GroupInfo 消费者组信息
type GroupInfo struct {
	Name            string         `json:"name"`
	Pending         int64          `json:"pending"`
	LastDeliveredID string         `json:"last_delivered_id"`
	Consumers       []ConsumerInfo `json:"consumers"`
}

// ConsumerInfo 消费者信息
type ConsumerInfo struct {
	Name    string        `json:"name"`
	Pending int64         `json:"pending"`
	Idle    time.Duration `json:"idle"`
}

// autoCleanupMessages 自动清理已处理的消息
func (mq *MessageQueue) autoCleanupMessages(ctx context.Context) {
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
func (mq *MessageQueue) performCleanup(ctx context.Context) {
	policy := mq.GetCleanupPolicy()
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

// findCleanableMessages 查找可以清理的消息
func (mq *MessageQueue) findCleanableMessages(ctx context.Context, policy *CleanupPolicy) ([]string, error) {
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
			if mq.isMessageOldEnough(msg.ID, policy.MinRetentionTime) {
				cleanableMessages = append(cleanableMessages, msg.ID)
			}
		}
	}

	return cleanableMessages, nil
}

// isMessageFullyAcked 检查消息是否被所有消费者组确认
func (mq *MessageQueue) isMessageFullyAcked(ctx context.Context, messageID string, groups []redis.XInfoGroup) bool {
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

// isMessageOldEnough 检查消息是否足够老可以被清理
func (mq *MessageQueue) isMessageOldEnough(messageID string, minRetentionTime time.Duration) bool {
	// 从消息ID中提取时间戳
	timestamp, err := extractTimestampFromMessageID(messageID)
	if err != nil {
		log.Printf("提取消息时间戳失败 %s: %v", messageID, err)
		return false
	}

	messageTime := time.Unix(timestamp/1000, (timestamp%1000)*1000000)
	return time.Since(messageTime) >= minRetentionTime
}

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

// cleanMessages 清理指定的消息
func (mq *MessageQueue) cleanMessages(ctx context.Context, messageIDs []string, batchSize int64) (int, error) {
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

// CleanupMessages 手动清理消息
func (mq *MessageQueue) CleanupMessages(ctx context.Context) (int, error) {
	policy := mq.GetCleanupPolicy()

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
