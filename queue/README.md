# Redis Stream消息队列实现

## 重试和死信队列功能

本项目实现了基于Redis Stream的消息队列，并支持消息重试和死信队列功能。

### 主要特性

1. **消息重试机制**：
   - 当消息处理失败时，自动安排重试
   - 使用指数退避算法设置重试间隔时间
   - 最大重试次数可配置（默认3次）
   - 重试消息保留原始消息ID和处理历史

2. **死信队列**：
   - 当消息重试次数超过最大限制时，自动发送到死信队列
   - 死信队列保留完整的消息内容和失败历史
   - 可通过独立的消费者处理死信队列中的消息

3. **长时间未确认消息处理**：
   - 自动检测并处理长时间未确认（Pending）的消息
   - 将超时未确认的消息放入重试队列
   - 超时时间可配置（默认5分钟）

### 实现方式

1. **重试队列**：使用Redis的Sorted Set实现，Score为处理时间戳
2. **死信队列**：使用Redis的Stream实现，保持与原始队列相同的数据结构
3. **重试监控**：使用goroutine定期检查重试队列和pending列表

### 使用方法

```go
// 创建消息队列
queue := NewMessageQueue(redisClient, "stream-name", "group-name", "consumer-1")

// 注册消息处理器
queue.RegisterHandler(&MyHandler{})

// 启动消息队列（会自动启动重试和监控）
queue.Start(ctx)

// 发布消息
producer := NewProducer(redisClient, "stream-name")
producer.PublishMessage(ctx, "message-type", data, metadata)
```

### 测试

提供了两个测试用例：

1. `TestRetryAndDeadLetterQueue`: 测试消息重试和死信队列功能
2. `TestLongPendingMessages`: 测试长时间未确认消息的处理

### 配置选项

- `TEST_MODE=1`: 设置为测试模式，使用更短的超时和重试间隔
- 重试间隔: 正常模式下为1分钟的指数退避，测试模式下为1秒的指数退避
- 最大重试次数: 默认为3次 