# 消息队列错误处理指南

## 🚨 Topic删除时消费者可能遇到的错误

当消费者正在处理消息时，如果topic（Redis Stream）被删除，会产生以下错误：

### 1. XREADGROUP 错误
**最常见的错误**，发生在消费者尝试读取新消息时：

```
NOGROUP No such key 'stream-name' or consumer group 'group-name' in XREADGROUP with GROUP option
```

**原因**：
- Stream被删除
- 消费者组被删除
- 两者都被删除

**影响**：消费者无法读取新消息

### 2. XACK 错误
发生在消费者尝试确认消息时：

```
ERR no such key
```

**原因**：Stream已被删除，无法ACK消息

**影响**：消息无法被正确确认，但不会影响已处理的消息

### 3. XPENDING 错误
发生在查询pending消息时：

```
ERR no such key
NOGROUP No such key 'stream-name' or consumer group 'group-name'
```

**原因**：Stream或消费者组不存在

**影响**：无法获取pending消息列表

### 4. XCLAIM 错误
发生在尝试抢夺pending消息时：

```
NOGROUP No such key 'stream-name' or consumer group 'group-name' in XCLAIM
```

**原因**：Stream或消费者组已被删除

**影响**：无法抢夺其他消费者的pending消息

### 5. XINFO 错误
发生在获取Stream信息时：

```
ERR no such key
```

**原因**：Stream不存在

**影响**：无法获取topic状态信息

## 🛡️ 错误处理机制

我们的消息队列系统实现了以下错误处理机制：

### 1. 自动检测和停止
```go
// 在读取消息时检测错误
if err != nil {
    if err != redis.Nil {
        log.Printf("读取新消息失败: %v", err)
        
        // 检查是否是因为Stream或消费者组被删除
        if isStreamOrGroupDeletedError(err) {
            log.Printf("检测到Stream或消费者组已被删除，停止消费者")
            return // 停止消费者
        }
    }
    continue
}
```

### 2. 错误模式识别
```go
func isStreamOrGroupDeletedError(err error) bool {
    if err == nil {
        return false
    }
    
    errStr := err.Error()
    
    // 常见的错误模式
    patterns := []string{
        "ERR no such key",
        "NOGROUP",
        "No such key", 
        "consumer group",
        "does not exist",
    }
    
    for _, pattern := range patterns {
        if contains(errStr, pattern) {
            return true
        }
    }
    
    return false
}
```

### 3. 优雅降级
- **继续处理**：如果是临时网络错误，继续重试
- **停止消费者**：如果检测到topic被删除，优雅停止
- **记录日志**：详细记录错误信息便于调试

## 📊 错误场景时间线

### 场景：消费者正在处理消息时topic被删除

```
时间 0s:  消费者启动，开始处理消息
时间 5s:  消费者正在处理消息A（需要8秒）
时间 8s:  管理员删除topic
时间 10s: 消费者尝试读取新消息 → NOGROUP错误
时间 10s: 系统检测到错误，停止消费者
时间 13s: 消息A处理完成，尝试ACK → ERR no such key
时间 13s: ACK失败，但消息已被处理
```

### 关键观察点

1. **正在处理的消息**：会继续处理完成
2. **ACK操作**：会失败，但不影响业务逻辑
3. **新消息读取**：立即失败并停止消费者
4. **Pending消息**：无法继续处理

## 🔧 最佳实践

### 1. 实现健康检查
```go
func (mq *MessageQueue) HealthCheck(ctx context.Context) error {
    // 检查Stream是否存在
    _, err := mq.client.XInfoStream(ctx, mq.streamName).Result()
    if err != nil {
        return fmt.Errorf("Stream健康检查失败: %w", err)
    }
    
    // 检查消费者组是否存在
    groups, err := mq.client.XInfoGroups(ctx, mq.streamName).Result()
    if err != nil {
        return fmt.Errorf("消费者组健康检查失败: %w", err)
    }
    
    // 检查当前消费者组是否存在
    for _, group := range groups {
        if group.Name == mq.groupName {
            return nil // 健康
        }
    }
    
    return fmt.Errorf("消费者组 %s 不存在", mq.groupName)
}
```

### 2. 实现重连机制
```go
func (mq *MessageQueue) StartWithRetry(ctx context.Context, maxRetries int) error {
    for i := 0; i < maxRetries; i++ {
        err := mq.Start(ctx)
        if err == nil {
            return nil
        }
        
        log.Printf("启动失败，第 %d 次重试: %v", i+1, err)
        time.Sleep(time.Second * time.Duration(i+1))
    }
    
    return fmt.Errorf("达到最大重试次数")
}
```

### 3. 监控和告警
```go
func (mq *MessageQueue) MonitorHealth(ctx context.Context) {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            err := mq.HealthCheck(ctx)
            if err != nil {
                // 发送告警
                log.Printf("健康检查失败: %v", err)
                // 可以集成告警系统
            }
        }
    }
}
```

### 4. 消息处理幂等性
```go
func (h *MyHandler) Handle(ctx context.Context, msg *queue.Message) error {
    // 检查消息是否已处理（防止重复处理）
    if isProcessed(msg.ID) {
        return nil
    }
    
    // 处理消息
    err := processMessage(msg)
    if err != nil {
        return err
    }
    
    // 标记为已处理
    markAsProcessed(msg.ID)
    return nil
}
```

## 🧪 测试错误处理

运行错误处理测试：

```bash
# 启动Redis
make redis-up

# 运行错误处理测试
make test-errors
```

这个测试会：
1. 启动消费者处理消息
2. 在处理过程中删除topic
3. 观察各种错误的产生和处理
4. 验证错误处理机制的有效性

## 📝 错误日志示例

正常的错误处理日志应该类似：

```
2024/01/01 10:00:00 开始处理新消息...
2024/01/01 10:00:05 🐌 开始慢速处理邮件消息: 1234567890-0
2024/01/01 10:00:08 开始终止topic: error-demo
2024/01/01 10:00:08 已删除消费者组: error-group
2024/01/01 10:00:08 已删除Stream: error-demo，清空了所有消息
2024/01/01 10:00:10 读取新消息失败: NOGROUP No such key 'error-demo' or consumer group 'error-group' in XREADGROUP with GROUP option
2024/01/01 10:00:10 检测到Stream或消费者组已被删除，停止消费者
2024/01/01 10:00:13 ✅ 邮件处理完成: 1234567890-0
2024/01/01 10:00:13 ACK消息失败 1234567890-0: ERR no such key
2024/01/01 10:00:13 检测到Stream或消费者组已被删除，ACK操作失败
```

## 🎯 总结

当topic被删除时，消费者会遇到多种错误，但我们的系统能够：

1. **自动检测**：识别topic删除相关的错误
2. **优雅停止**：停止消费者避免无效操作
3. **完成处理**：让正在处理的消息完成
4. **详细日志**：记录所有错误便于调试
5. **健康检查**：提供监控和告警机制

这确保了系统在异常情况下的稳定性和可靠性。 