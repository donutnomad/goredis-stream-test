# 🛑 消费者自动停止功能

## 📋 功能概述

goStream消息队列系统现在支持智能的消费者自动停止功能。当Redis Stream或消费者组被删除时，消费者会自动检测并优雅停止，避免无效的重试和错误日志。

## 🎯 核心特性

### 1. 智能检测
- **自动检测**：实时监控Redis Stream和消费者组的状态
- **错误识别**：准确识别因topic删除导致的错误
- **即时响应**：检测到删除后立即触发自动停止

### 2. 优雅停止
- **资源清理**：自动清理所有相关资源和goroutine
- **状态管理**：正确管理消费者的内部状态
- **避免重试**：停止无效的重试操作

### 3. 外部监听
- **Done()通道**：提供阻塞通道供外部监听
- **状态查询**：通过IsAutoStopped()查询停止原因
- **事件通知**：及时通知外部系统消费者状态变化

## 🔧 技术实现

### 核心字段
```go
type MessageQueue struct {
    // ... 其他字段 ...
    
    // 自动停止相关
    doneChan     chan struct{} // 用于通知外部消费者已停止
    autoStopped  bool          // 标记是否因为topic删除而自动停止
    autoStopOnce sync.Once     // 确保只自动停止一次
}
```

### 关键方法

#### Done() - 获取停止通知通道
```go
func (mq *MessageQueue) Done() <-chan struct{} {
    return mq.doneChan
}
```

#### IsAutoStopped() - 检查是否自动停止
```go
func (mq *MessageQueue) IsAutoStopped() bool {
    mq.mu.RLock()
    defer mq.mu.RUnlock()
    return mq.autoStopped
}
```

#### autoStop() - 执行自动停止
```go
func (mq *MessageQueue) autoStop(reason string) {
    mq.autoStopOnce.Do(func() {
        // 设置状态
        mq.autoStopped = true
        mq.stopped = true
        
        // 停止所有goroutine
        close(mq.stopChan)
        
        // 通知外部
        close(mq.doneChan)
        
        // 等待清理完成
        mq.wg.Wait()
    })
}
```

### 错误检测逻辑
```go
func isStreamOrGroupDeletedError(err error) bool {
    if err == nil {
        return false
    }
    
    errStr := err.Error()
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

## 💡 使用示例

### 基本使用
```go
// 创建消费者
consumer := queue.NewMessageQueue(rdb, "my-stream", "my-group", "consumer-1")
consumer.RegisterHandler(&MyHandler{})

// 启动消费者
err := consumer.Start(ctx)
if err != nil {
    log.Fatal(err)
}

// 监听自动停止
go func() {
    <-consumer.Done()
    if consumer.IsAutoStopped() {
        log.Println("✅ 消费者因topic删除而自动停止")
        // 执行清理逻辑
    } else {
        log.Println("ℹ️  消费者手动停止")
    }
}()
```

### 微服务场景
```go
type ServiceManager struct {
    consumers []*queue.MessageQueue
    wg        sync.WaitGroup
}

func (sm *ServiceManager) StartConsumers() {
    for _, consumer := range sm.consumers {
        sm.wg.Add(1)
        go func(c *queue.MessageQueue) {
            defer sm.wg.Done()
            
            // 启动消费者
            c.Start(ctx)
            
            // 监听自动停止
            <-c.Done()
            if c.IsAutoStopped() {
                log.Printf("消费者 %s 因topic删除而自动停止", c.GetStreamName())
                // 从服务列表中移除
                sm.removeConsumer(c)
            }
        }(consumer)
    }
}

func (sm *ServiceManager) Shutdown() {
    // 停止所有消费者
    for _, consumer := range sm.consumers {
        consumer.Stop()
    }
    
    // 等待所有消费者停止
    sm.wg.Wait()
}
```

### 容器化环境
```go
func main() {
    // 创建消费者
    consumer := queue.NewMessageQueue(rdb, "task-stream", "task-group", "worker-1")
    consumer.RegisterHandler(&TaskHandler{})
    
    // 启动消费者
    consumer.Start(ctx)
    
    // 监听系统信号和自动停止
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    select {
    case <-sigChan:
        log.Println("收到系统信号，手动停止消费者")
        consumer.Stop()
    case <-consumer.Done():
        if consumer.IsAutoStopped() {
            log.Println("topic被删除，消费者自动停止")
        }
    }
    
    log.Println("服务已停止")
}
```

## 🧪 测试用例

### 单消费者自动停止测试
```go
func TestConsumerAutoStop(t *testing.T) {
    // 创建并启动消费者
    consumer := queue.NewMessageQueue(rdb, streamName, groupName, "auto-stop-consumer")
    consumer.Start(ctx)
    
    // 监控自动停止
    go func() {
        <-consumer.Done()
        assert.True(t, consumer.IsAutoStopped())
    }()
    
    // 删除topic触发自动停止
    terminator := queue.NewMessageQueue(rdb, streamName, groupName, "terminator")
    terminator.TerminateTopic(ctx)
    
    // 验证结果
    assert.True(t, consumer.IsAutoStopped())
}
```

### 多消费者自动停止测试
```go
func TestMultipleConsumersAutoStop(t *testing.T) {
    // 创建多个消费者
    consumers := make([]*queue.MessageQueue, 3)
    for i := 0; i < 3; i++ {
        consumer := queue.NewMessageQueue(rdb, streamName, groupName, fmt.Sprintf("consumer-%d", i))
        consumer.Start(ctx)
        consumers[i] = consumer
    }
    
    // 监控所有消费者
    var wg sync.WaitGroup
    for _, consumer := range consumers {
        wg.Add(1)
        go func(c *queue.MessageQueue) {
            defer wg.Done()
            <-c.Done()
            assert.True(t, c.IsAutoStopped())
        }(consumer)
    }
    
    // 删除topic
    terminator := queue.NewMessageQueue(rdb, streamName, groupName, "terminator")
    terminator.TerminateTopic(ctx)
    
    // 等待所有消费者停止
    wg.Wait()
}
```

### 自动停止与手动停止区别测试
```go
func TestAutoStopVsManualStop(t *testing.T) {
    // 测试手动停止
    consumer1 := queue.NewMessageQueue(rdb, "manual-test", "group", "consumer")
    consumer1.Start(ctx)
    consumer1.Stop() // 手动停止
    assert.False(t, consumer1.IsAutoStopped())
    
    // 测试自动停止
    consumer2 := queue.NewMessageQueue(rdb, "auto-test", "group", "consumer")
    consumer2.Start(ctx)
    
    terminator := queue.NewMessageQueue(rdb, "auto-test", "group", "terminator")
    terminator.TerminateTopic(ctx) // 触发自动停止
    
    <-consumer2.Done()
    assert.True(t, consumer2.IsAutoStopped())
}
```

## 🚀 使用场景

### 1. 微服务架构
- **动态扩缩容**：当topic被删除时，相关消费者自动停止
- **服务发现**：消费者可以自动从服务注册中心注销
- **资源管理**：避免僵尸消费者占用资源

### 2. 容器化环境
- **优雅关闭**：容器停止时消费者能够优雅退出
- **健康检查**：通过Done()通道实现健康检查
- **故障恢复**：快速检测并响应基础设施故障

### 3. 开发和测试
- **测试隔离**：测试结束后自动清理消费者
- **开发调试**：避免因topic删除导致的错误日志干扰
- **CI/CD流水线**：自动化测试中的资源清理

### 4. 运维管理
- **维护操作**：维护期间删除topic时消费者自动停止
- **监控告警**：通过自动停止事件触发监控告警
- **日志管理**：减少无效的错误日志

## 📊 性能影响

### 检测开销
- **最小开销**：只在Redis操作出错时进行检测
- **高效判断**：使用字符串匹配快速识别错误类型
- **无额外请求**：不增加额外的Redis请求

### 内存使用
- **轻量级**：只增加少量字段和通道
- **及时清理**：自动停止后立即释放资源
- **无泄漏**：确保所有goroutine正确退出

### 响应时间
- **即时响应**：检测到错误后立即触发停止
- **并发安全**：使用sync.Once确保只停止一次
- **优雅退出**：等待所有goroutine完成后退出

## 🔍 故障排查

### 常见问题

#### 1. 消费者未自动停止
**可能原因**：
- Redis连接异常
- 错误类型未被正确识别
- 消费者已经手动停止

**排查方法**：
```go
// 检查错误日志
log.Printf("Redis错误: %v", err)

// 检查消费者状态
if consumer.IsAutoStopped() {
    log.Println("消费者已自动停止")
} else {
    log.Println("消费者未自动停止")
}
```

#### 2. Done()通道未关闭
**可能原因**：
- 消费者未启动
- 自动停止逻辑未触发
- 程序异常退出

**排查方法**：
```go
// 使用超时检测
select {
case <-consumer.Done():
    log.Println("消费者已停止")
case <-time.After(time.Second * 10):
    log.Println("等待超时，消费者可能未正常停止")
}
```

#### 3. 内存泄漏
**可能原因**：
- goroutine未正确退出
- 通道未关闭
- 资源未释放

**排查方法**：
```go
// 检查goroutine数量
runtime.GC()
log.Printf("Goroutine数量: %d", runtime.NumGoroutine())

// 使用pprof分析
import _ "net/http/pprof"
go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
}()
```

## 📈 最佳实践

### 1. 监听模式
```go
// 推荐：使用select监听多个事件
select {
case <-consumer.Done():
    handleConsumerStop(consumer)
case <-ctx.Done():
    handleContextCancel()
case <-sigChan:
    handleSignal()
}
```

### 2. 错误处理
```go
// 推荐：区分不同的停止原因
<-consumer.Done()
if consumer.IsAutoStopped() {
    // 自动停止：可能需要重新创建消费者
    log.Println("Topic被删除，需要重新创建")
} else {
    // 手动停止：正常的程序退出
    log.Println("程序正常退出")
}
```

### 3. 资源管理
```go
// 推荐：使用defer确保资源清理
func startConsumer() {
    consumer := queue.NewMessageQueue(...)
    defer func() {
        if !consumer.IsAutoStopped() {
            consumer.Stop()
        }
    }()
    
    consumer.Start(ctx)
    <-consumer.Done()
}
```

### 4. 测试覆盖
```go
// 推荐：测试所有停止场景
func TestAllStopScenarios(t *testing.T) {
    // 测试手动停止
    testManualStop(t)
    
    // 测试自动停止
    testAutoStop(t)
    
    // 测试并发停止
    testConcurrentStop(t)
    
    // 测试异常停止
    testErrorStop(t)
}
```

## 🎉 总结

消费者自动停止功能为goStream消息队列系统提供了更加智能和健壮的错误处理能力。通过自动检测topic删除事件并优雅停止消费者，系统能够：

1. **提高稳定性**：避免无效重试和错误日志
2. **简化运维**：自动处理常见的运维场景
3. **增强监控**：提供清晰的状态变化事件
4. **优化资源**：及时释放不再需要的资源

这个功能特别适合在微服务架构、容器化环境和自动化运维场景中使用，能够显著提升系统的可靠性和可维护性。 