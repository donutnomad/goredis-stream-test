package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
)

// ILogger 日志接口
type ILogger interface {
	Printf(format string, v ...interface{})
}

// defaultLogger 默认日志实现，使用标准库log
type defaultLogger struct{}

func (l *defaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// loggerWrapper 日志包装器，添加前缀
type loggerWrapper struct {
	logger ILogger
	prefix string
}

func newLoggerWrapper(logger ILogger, prefix string) *loggerWrapper {
	return &loggerWrapper{
		logger: logger,
		prefix: prefix,
	}
}

func (w *loggerWrapper) Printf(format string, v ...interface{}) {
	w.logger.Printf(w.prefix+" "+format, v...)
}

// Message 消息结构
type Message struct {
	ID       string                 `json:"id"`       // 消息ID，来自Redis的消息ID
	Type     string                 `json:"type"`     // 消息类型
	Data     map[string]interface{} `json:"data"`     // 消息数据
	Metadata map[string]string      `json:"metadata"` // 消息元数据
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

// RedisMessagePayload Redis消息载荷结构
type RedisMessagePayload struct {
	Type     string                 `json:"type"`
	Data     map[string]interface{} `json:"data"`
	Metadata map[string]string      `json:"metadata"`
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
	cleaner *MessageCleaner

	// 批量处理相关
	batchHandlers map[string]BatchMessageHandler
	batchConfig   *BatchConfig

	// 自动停止相关
	doneChan    chan struct{} // 用于通知外部消费者已停止
	autoStopped bool          // 标记是否因为topic删除而自动停止

	// 日志
	logger ILogger

	// 重试相关
	maxRetries int // 最大重试次数，默认为3
}

// MessageQueueOption 消息队列配置选项
type MessageQueueOption func(*MessageQueue)

// WithLogger 设置自定义日志器
func WithLogger(logger ILogger) MessageQueueOption {
	return func(mq *MessageQueue) {
		// 使用包装器包装传入的logger
		mq.logger = newLoggerWrapper(logger, "[mq]")
	}
}

// WithBatchConfig 设置批量处理配置
func WithBatchConfig(config *BatchConfig) MessageQueueOption {
	return func(mq *MessageQueue) {
		mq.batchConfig = config
	}
}

func newMessageQueue(client *redis.Client, streamName, groupName, consumerName string, startPos StartPosition, specificID string, options ...MessageQueueOption) *MessageQueue {
	cleaner := NewMessageCleaner(client, streamName, consumerName)
	baseLogger := &defaultLogger{}
	wrappedLogger := newLoggerWrapper(baseLogger, "[mq]")
	mq := &MessageQueue{
		client:        client,
		streamName:    streamName,
		groupName:     groupName,
		consumerName:  consumerName,
		startPos:      startPos,
		specificID:    specificID,
		handlers:      make(map[string]MessageHandler),
		stopChan:      make(chan struct{}),
		cleaner:       cleaner,
		batchHandlers: make(map[string]BatchMessageHandler),
		batchConfig:   DefaultBatchConfig(),
		doneChan:      make(chan struct{}),
		logger:        wrappedLogger,
	}
	for _, option := range options {
		option(mq)
	}
	return mq
}

// NewMessageQueue 创建新的消息队列（默认从最新位置开始）
func NewMessageQueue(client *redis.Client, streamName, groupName, consumerName string, options ...MessageQueueOption) *MessageQueue {
	return newMessageQueue(client, streamName, groupName, consumerName, StartFromLatest, "", options...)
}

// NewMessageQueueWithStartPosition 创建新的消息队列并指定开始位置
func NewMessageQueueWithStartPosition(client *redis.Client, streamName, groupName, consumerName string, startPos StartPosition, options ...MessageQueueOption) *MessageQueue {
	return newMessageQueue(client, streamName, groupName, consumerName, startPos, "", options...)
}

// NewMessageQueueFromSpecificID 创建新的消息队列并从指定消息ID开始
func NewMessageQueueFromSpecificID(client *redis.Client, streamName, groupName, consumerName string, messageID string, options ...MessageQueueOption) *MessageQueue {
	return newMessageQueue(client, streamName, groupName, consumerName, StartFromSpecific, messageID, options...)
}

func (mq *MessageQueue) GetCleaner() *MessageCleaner {
	return mq.cleaner
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

	// 启动监控重试队列的goroutine
	mq.wg.Add(1)
	go mq.monitorRetryQueue(ctx)

	// 启动监控长时间未确认消息的goroutine
	mq.wg.Add(1)
	go mq.monitorLongPendingMessages(ctx)

	// 清理有关的
	mq.cleaner.OnStart(ctx)

	mq.logger.Printf("消息队列已启动 - Stream: %s, Group: %s, Consumer: %s",
		mq.streamName, mq.groupName, mq.consumerName)

	return nil
}

// stop 内部统一的停止方法
func (mq *MessageQueue) stop(isAuto bool, reason string) {
	mq.mu.Lock()
	if mq.stopped {
		mq.mu.Unlock()
		return
	}

	if isAuto {
		mq.autoStopped = true
	}
	mq.stopped = true
	mq.mu.Unlock()

	if isAuto {
		mq.logger.Printf("消费者 %s 自动停止: %s", mq.consumerName, reason)
	}

	mq.stopInternal()

	// 简化停止完成日志
	if isAuto {
		mq.logger.Printf("消费者 %s 已停止", mq.consumerName)
	} else {
		mq.logger.Printf("消息队列已停止")
	}
}

// autoStop 自动停止消费者（因为topic被删除）
func (mq *MessageQueue) autoStop(reason string) {
	mq.stop(true, reason)
}

// Stop 停止消息队列
func (mq *MessageQueue) Stop() {
	mq.stop(false, "")
}

// stopInternal 内部停止方法，处理共同的停止逻辑
func (mq *MessageQueue) stopInternal() {
	// 关闭停止通道以通知所有goroutine停止
	close(mq.stopChan)

	// 停止清理器
	mq.cleaner.Stop()

	// 如果不是自动停止，则关闭doneChan
	// 如果是自动停止，doneChan已经在autoStop中关闭
	if !mq.autoStopped {
		close(mq.doneChan)
	} else {
		// 如果是自动停止，确保doneChan被关闭
		select {
		case <-mq.doneChan:
			// 通道已经关闭，不需要操作
		default:
			// 通道未关闭，关闭它
			close(mq.doneChan)
		}
	}

	// 等待所有goroutine结束
	mq.wg.Wait()
	mq.cleaner.Wait()
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

	mq.logger.Printf("创建消费者组 %s，开始位置: %s", mq.groupName, startID)

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
					mq.logger.Printf("检测到Stream或消费者组已被删除，停止处理")
					go mq.autoStop("Stream或消费者组被删除")
					return
				}

				time.Sleep(time.Second)
				continue
			}
			if len(pending) == 0 {
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
		// 保留错误日志，这是重要的错误信息
		mq.logger.Printf("声明消息所有权失败 %s: %v", messageID, err)
		return
	}
	if len(claimed) == 0 {
		return
	}
	// 处理消息
	parsedMessages := mq.parseMessages(ctx, claimed)
	// 统一调用消息处理方法
	mq.handleMessages(ctx, parsedMessages)
}

// processNewMessages 处理新消息
func (mq *MessageQueue) processNewMessages(ctx context.Context) {
	defer mq.wg.Done()
	// 直接处理消息，不再分别调用两个不同的方法
	mq.processMessages(ctx, mq.GetBatchConfig())
}

func (mq *MessageQueue) processMessages(ctx context.Context, batchConfig *BatchConfig) {
	batchSize := 1
	blockTimeout := time.Second * 5
	if batchConfig.EnableBatch {
		batchSize = batchConfig.BatchSize
		blockTimeout = batchConfig.BatchTimeout
	}

	for {
		select {
		case <-mq.stopChan:
			return
		case <-ctx.Done():
			return
		default:
			// 读取消息
			streams, err := mq.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    mq.groupName,
				Consumer: mq.consumerName,
				Streams:  []string{mq.streamName, ">"},
				Count:    int64(batchSize),
				Block:    blockTimeout,
			}).Result()
			if err != nil {
				if !errors.Is(err, redis.Nil) {
					mq.logger.Printf("读取消息失败: %v", err)
					// 检查是否是因为Stream或消费者组被删除
					if isStreamOrGroupDeletedError(err) {
						mq.logger.Printf("检测到Stream或消费者组已被删除，停止消费者")
						go mq.autoStop("Stream或消费者组被删除")
						return
					}
				}
				continue
			}
			for _, stream := range streams {
				if len(stream.Messages) == 0 {
					continue
				}
				parsedMessages := mq.parseMessages(ctx, stream.Messages)
				mq.handleMessages(ctx, parsedMessages)
			}
		}
	}
}

// handleMessages 处理消息，支持单条和批量处理
func (mq *MessageQueue) handleMessages(ctx context.Context, messages []*Message) {
	if len(messages) == 0 {
		return
	}
	messageGroups := lo.GroupBy(messages, func(item *Message) string {
		return item.Type
	})
	for msgType, typedMessages := range messageGroups {
		single, batch := mq.getHandlerByType(msgType)
		switch {
		case batch != nil:
			batchSize := (*batch).GetBatchSize()
			if batchSize <= 0 {
				batchSize = mq.GetBatchConfig().BatchSize
			}
			for i := 0; i < len(typedMessages); i += batchSize {
				end := min(i+batchSize, len(typedMessages))
				batchMsg := typedMessages[i:end]
				err := (*batch).HandleBatch(ctx, batchMsg)
				if err != nil {
					mq.logger.Printf("批量处理消息失败 类型=%s: %v", msgType, err)
					// 为每条消息安排重试
					for _, msg := range batchMsg {
						mq.handleMessageFailure(ctx, msg, err)
					}
					continue
				}
				mq.ackMessages(ctx, extractIds(batchMsg))
			}
		case single != nil:
			// 回退到单条处理
			mq.logger.Printf("未找到批量处理器，回退到单条处理: 类型=%s, 数量=%d", msgType, len(typedMessages))
			for _, message := range typedMessages {
				err := (*single).Handle(ctx, message)
				if err != nil {
					// 使用新的错误处理逻辑
					mq.handleMessageFailure(ctx, message, err)
					continue
				}
				mq.ackMessage(ctx, message.ID)
			}
		default:
			mq.logger.Printf("no handler found for message type: %s", msgType)
			mq.ackMessages(ctx, extractIds(typedMessages))
		}
	}
}

func (mq *MessageQueue) getHandlerByType(messageType string) (singleHandler *MessageHandler, batchHandler *BatchMessageHandler) {
	mq.mu.RLock()
	singleHandler_, exists := mq.handlers[messageType]
	if exists {
		singleHandler = &singleHandler_
	}
	batchHandler_, exists := mq.batchHandlers[messageType]
	if exists {
		batchHandler = &batchHandler_
	}
	mq.mu.RUnlock()
	return singleHandler, batchHandler
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

// parseMessages 批量解析消息并处理错误
func (mq *MessageQueue) parseMessages(ctx context.Context, redisMessages []redis.XMessage) []*Message {
	parsedMessages := make([]*Message, 0, len(redisMessages))
	for _, redisMsg := range redisMessages {
		// 解析消息
		message, err := mq.parseMessage(redisMsg)
		if err != nil {
			mq.logger.Printf("解析消息失败 %s: %v", redisMsg.ID, err)
			mq.ackMessage(ctx, redisMsg.ID)
			continue
		}
		parsedMessages = append(parsedMessages, message)
	}
	return parsedMessages
}

// ackMessage ACK消息
func (mq *MessageQueue) ackMessage(ctx context.Context, messageID string) {
	err := mq.client.XAck(ctx, mq.streamName, mq.groupName, messageID).Err()
	if err != nil {
		// 保留ACK失败的错误日志，这是重要的错误信息
		mq.logger.Printf("ACK消息失败 %s: %v", messageID, err)
		// 检查是否是因为Stream或消费者组被删除
		if isStreamOrGroupDeletedError(err) {
			mq.logger.Printf("检测到Stream或消费者组已被删除，ACK操作失败")
			go mq.autoStop("Stream或消费者组被删除")
		} else {
			// TODO: 添加一些重试机制
		}
	}
}

func (mq *MessageQueue) ackMessages(ctx context.Context, messageIDs []string) {
	for _, messageID := range messageIDs {
		mq.ackMessage(ctx, messageID)
	}
}

// GetTopicInfo 获取topic信息（消息数量、消费者组等）
func (mq *MessageQueue) GetTopicInfo(ctx context.Context) (*TopicInfo, error) {
	return getTopicInfo(ctx, mq.client, mq.streamName)
}

// TerminateTopic 终止整个topic，清空所有消息和消费者组
func (mq *MessageQueue) TerminateTopic(ctx context.Context) error {
	mq.logger.Printf("终止topic: %s", mq.streamName)

	// 首先停止当前消息队列的处理
	mq.Stop()

	// 获取所有消费者组信息
	groups, err := mq.client.XInfoGroups(ctx, mq.streamName).Result()
	if err != nil && err.Error() != "ERR no such key" {
		mq.logger.Printf("获取消费者组信息失败: %v", err)
	} else {
		// 删除所有消费者组
		for _, group := range groups {
			err = mq.client.XGroupDestroy(ctx, mq.streamName, group.Name).Err()
			if err != nil {
				mq.logger.Printf("删除消费者组 %s 失败: %v", group.Name, err)
			}
		}
	}

	// 删除整个Stream（这会清空所有消息）
	deleted, err := mq.client.Del(ctx, mq.streamName).Result()
	if err != nil {
		return fmt.Errorf("删除Stream失败: %w", err)
	}

	// 简化Stream删除日志
	if deleted > 0 {
		mq.logger.Printf("已删除Stream: %s", mq.streamName)
	}

	return nil
}

// getRetryQueueName 获取重试队列名称
// 重试队列使用Redis的SortedSet实现，分数为处理时间戳
func (mq *MessageQueue) getRetryQueueName() string {
	return mq.streamName + ".retry"
}

// getDeadLetterQueueName 获取死信队列名称
// 死信队列使用Redis的Stream实现，存储重试次数超过上限的消息
func (mq *MessageQueue) getDeadLetterQueueName() string {
	return mq.streamName + ".dlq"
}

// monitorLongPendingMessages 监控长时间未确认的消息
// 这个方法会定期检查消费者组中长时间未被确认的消息，并将它们放入重试队列
// 在测试模式下，超时时间为3秒，正常模式下为5分钟
func (mq *MessageQueue) monitorLongPendingMessages(ctx context.Context) {
	defer mq.wg.Done()

	// 定义超时时间
	pendingTimeout := time.Minute * 5
	// 测试模式下使用更短的超时时间
	if os.Getenv("TEST_MODE") == "1" {
		pendingTimeout = time.Second * 3
	}

	// 定时检查
	ticker := time.NewTicker(time.Minute) // 每分钟检查一次
	if os.Getenv("TEST_MODE") == "1" {
		ticker = time.NewTicker(time.Second) // 测试模式下每秒检查一次
	}
	defer ticker.Stop()

	for {
		select {
		case <-mq.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 查找超时的pending消息
			longPendingMessages, err := mq.client.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: mq.streamName,
				Group:  mq.groupName,
				Start:  "-",
				End:    "+",
				Count:  100,            // 一次最多处理100条
				Idle:   pendingTimeout, // 只返回空闲超过指定时间的消息
			}).Result()

			if err != nil {
				if isStreamOrGroupDeletedError(err) {
					mq.logger.Printf("检测到Stream或消费者组已被删除，停止监控")
					go mq.autoStop("Stream或消费者组被删除")
					return
				}
				mq.logger.Printf("获取长时间pending消息失败: %v", err)
				continue
			}

			if len(longPendingMessages) == 0 {
				continue // 没有超时消息，继续等待
			}

			mq.logger.Printf("发现 %d 条超过 %v 未确认的消息，准备重新处理",
				len(longPendingMessages), pendingTimeout)

			// 处理每条超时消息
			for _, p := range longPendingMessages {
				select {
				case <-mq.stopChan:
					return
				case <-ctx.Done():
					return
				default:
					// 声明消息所有权
					claimed, err := mq.client.XClaim(ctx, &redis.XClaimArgs{
						Stream:   mq.streamName,
						Group:    mq.groupName,
						Consumer: mq.consumerName,
						MinIdle:  pendingTimeout,
						Messages: []string{p.ID},
					}).Result()

					if err != nil {
						mq.logger.Printf("声明超时消息所有权失败 %s: %v", p.ID, err)
						continue
					}

					if len(claimed) == 0 {
						continue
					}

					// 处理每条声明的消息
					for _, msg := range claimed {
						message, err := mq.parseMessage(msg)
						if err != nil {
							mq.logger.Printf("解析超时消息失败 %s: %v", msg.ID, err)
							mq.ackMessage(ctx, msg.ID) // 确认错误消息，避免重复处理
							continue
						}

						// 确保Metadata已初始化
						if message.Metadata == nil {
							message.Metadata = make(map[string]string)
						}

						// 获取重试次数
						retryCount := 0
						if retryCountStr, ok := message.Metadata["retry_count"]; ok {
							retryCount, _ = strconv.Atoi(retryCountStr)
						}

						// 判断是否超过最大重试次数
						maxRetries := 3 // 默认最大重试次数
						if mq.maxRetries > 0 {
							maxRetries = mq.maxRetries
						}

						if retryCount >= maxRetries {
							// 发送到死信队列
							mq.logger.Printf("消息 %s 已超过最大重试次数 %d，发送到死信队列",
								message.ID, maxRetries)
							mq.sendToDeadLetterQueue(ctx, message)
							mq.ackMessage(ctx, message.ID) // 确认原消息
						} else {
							// 更新重试信息
							message.Metadata["retry_count"] = strconv.Itoa(retryCount + 1)
							message.Metadata["last_retry_time"] = time.Now().Format(time.RFC3339)

							// 计算延迟时间（指数退避）
							var delaySeconds int
							if os.Getenv("TEST_MODE") == "1" {
								// 测试模式下使用更短的延迟 (1秒的指数退避)
								delaySeconds = int(math.Pow(2, float64(retryCount)))
								mq.logger.Printf("测试模式：设置延迟时间为 %d 秒", delaySeconds)
							} else {
								delaySeconds = int(math.Pow(2, float64(retryCount))) * 60 // 1分钟的指数退避
							}
							mq.scheduleRetry(ctx, message, delaySeconds)
							mq.ackMessage(ctx, message.ID) // 确认原消息
						}
					}
				}
			}
		}
	}
}

// handleMessageFailure 处理消息失败时的重试逻辑
// 当消息处理失败时，该方法会根据重试次数决定是放入重试队列还是死信队列
// 重试策略采用指数退避，在测试模式下为1秒的指数退避，正常模式下为1分钟的指数退避
func (mq *MessageQueue) handleMessageFailure(ctx context.Context, message *Message, err error) {
	mq.logger.Printf("处理消息失败 %s: %v", message.ID, err)

	// 获取重试次数
	retryCount := 0
	if retryCountStr, ok := message.Metadata["retry_count"]; ok {
		retryCount, _ = strconv.Atoi(retryCountStr)
	}

	// 更新重试信息
	message.Metadata["retry_count"] = strconv.Itoa(retryCount + 1)
	message.Metadata["last_error"] = err.Error()
	message.Metadata["last_retry_time"] = time.Now().Format(time.RFC3339)

	// 判断是否超过最大重试次数
	maxRetries := 3 // 默认最大重试次数
	if mq.maxRetries > 0 {
		maxRetries = mq.maxRetries
	}

	if retryCount >= maxRetries {
		// 发送到死信队列
		mq.sendToDeadLetterQueue(ctx, message)
	} else {
		// 计算延迟时间（指数退避）
		var delaySeconds int
		if os.Getenv("TEST_MODE") == "1" {
			// 测试模式下使用更短的延迟 (1秒的指数退避)
			delaySeconds = int(math.Pow(2, float64(retryCount)))
			mq.logger.Printf("测试模式：设置延迟时间为 %d 秒", delaySeconds)
		} else {
			delaySeconds = int(math.Pow(2, float64(retryCount))) * 60 // 1分钟的指数退避
		}
		mq.scheduleRetry(ctx, message, delaySeconds)
	}

	// 确认当前消息
	mq.ackMessage(ctx, message.ID)
}

// sendToDeadLetterQueue 发送消息到死信队列
// 当消息重试次数超过最大限制时，会被发送到死信队列
// 死信队列中的消息保留原始消息的所有信息，并添加失败相关的元数据
func (mq *MessageQueue) sendToDeadLetterQueue(ctx context.Context, message *Message) error {
	dlqName := mq.getDeadLetterQueueName()

	// 添加额外的元数据
	message.Metadata["original_id"] = message.ID
	message.Metadata["failure_time"] = time.Now().Format(time.RFC3339)

	// 序列化消息数据和元数据
	dataJson, err := json.Marshal(message.Data)
	if err != nil {
		return err
	}

	metadataJson, err := json.Marshal(message.Metadata)
	if err != nil {
		return err
	}

	// 将消息添加到死信队列Stream
	_, err = mq.client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqName,
		Values: map[string]interface{}{
			"type":     message.Type,
			"data":     string(dataJson),
			"metadata": string(metadataJson),
		},
	}).Result()

	if err != nil {
		mq.logger.Printf("发送消息到死信队列失败: %v", err)
		return err
	}

	mq.logger.Printf("消息 %s 已发送到死信队列 %s", message.ID, dlqName)
	return nil
}

// scheduleRetry 安排消息重试
// 将消息序列化后放入Redis的SortedSet中，分数为计划处理时间
// 重试延迟时间根据重试次数递增，采用指数退避算法
func (mq *MessageQueue) scheduleRetry(ctx context.Context, message *Message, delaySeconds int) error {
	retryQueueName := mq.getRetryQueueName()

	// 序列化整个消息
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// 计算执行时间（当前时间 + 延迟时间）
	processTime := time.Now().Add(time.Second * time.Duration(delaySeconds)).Unix()

	// 添加到Sorted Set，分数为处理时间
	err = mq.client.ZAdd(ctx, retryQueueName, redis.Z{
		Score:  float64(processTime),
		Member: string(messageBytes),
	}).Err()

	if err != nil {
		mq.logger.Printf("安排消息重试失败: %v", err)
		return err
	}

	mq.logger.Printf("消息 %s 已安排在 %d 秒后重试", message.ID, delaySeconds)
	return nil
}

// monitorRetryQueue 监控重试队列
// 这个方法会定期检查重试队列中是否有到期需要处理的消息
// 到期的消息会被重新放回原始Stream中进行处理
func (mq *MessageQueue) monitorRetryQueue(ctx context.Context) {
	defer mq.wg.Done()

	retryQueueName := mq.getRetryQueueName()
	pollInterval := time.Second * 5 // 每5秒检查一次

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mq.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 获取当前时间作为分数上限
			now := time.Now().Unix()

			// 查找所有应该处理的消息（分数 <= 当前时间）
			messages, err := mq.client.ZRangeByScore(ctx, retryQueueName, &redis.ZRangeBy{
				Min:    "0",
				Max:    fmt.Sprintf("%d", now),
				Offset: 0,
				Count:  10, // 一次处理10条
			}).Result()

			if err != nil {
				mq.logger.Printf("获取重试队列消息失败: %v", err)
				continue
			}

			if len(messages) == 0 {
				continue // 没有需要重试的消息
			}

			// 处理每条需要重试的消息
			for _, msgStr := range messages {
				// 反序列化消息
				var message Message
				if err := json.Unmarshal([]byte(msgStr), &message); err != nil {
					mq.logger.Printf("解析重试消息失败: %v", err)
					// 从重试队列中删除这条错误消息
					mq.client.ZRem(ctx, retryQueueName, msgStr)
					continue
				}

				// 将消息重新添加到原队列
				dataJson, _ := json.Marshal(message.Data)
				metadataJson, _ := json.Marshal(message.Metadata)

				// 保存原始ID
				oldID := message.ID
				if oldID != "" && message.Metadata["original_id"] == "" {
					message.Metadata["original_id"] = oldID
				}

				// 重新发送到原始队列
				newID, err := mq.client.XAdd(ctx, &redis.XAddArgs{
					Stream: mq.streamName,
					Values: map[string]interface{}{
						"type":     message.Type,
						"data":     string(dataJson),
						"metadata": string(metadataJson),
					},
				}).Result()

				if err != nil {
					mq.logger.Printf("重新入队失败: %v", err)
					continue
				}

				mq.logger.Printf("消息已重新入队，原ID: %s, 新ID: %s", oldID, newID)

				// 从重试队列中删除已处理的消息
				mq.client.ZRem(ctx, retryQueueName, msgStr)
			}
		}
	}
}
