# 消息队列系统使用指南

## 🚀 快速开始

### 1. 启动Redis服务
```bash
make redis-up
```

### 2. 运行示例程序
```bash
# 运行完整演示
make run

# 运行简单测试
make test

# 运行Pending消息处理演示
make test-pending

# 运行Topic终止功能演示
make test-terminate
```

## 📋 核心功能详解

### 1. 基本消息处理

#### 创建消息队列
```go
rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

mq := queue.NewMessageQueue(rdb, "my-stream", "my-group", "consumer-1")
```

#### 注册消息处理器
```go
mq.RegisterHandler(handlers.NewEmailHandler())
mq.RegisterHandler(handlers.NewOrderHandler())
```

#### 启动消费者
```go
ctx := context.Background()
err := mq.Start(ctx)
if err != nil {
    log.Fatal(err)
}
```

#### 发布消息
```go
messageID, err := mq.PublishMessage(ctx, "email", map[string]interface{}{
    "to":      "user@example.com",
    "subject": "欢迎注册",
    "body":    "欢迎您注册我们的服务！",
}, map[string]string{
    "priority": "high",
    "source":   "registration",
})
```

### 2. Pending消息处理

系统会自动处理Pending消息：

1. **消费者启动时**：优先处理PendingList中的消息
2. **消息抢夺**：使用XCLAIM抢夺超时的消息
3. **故障恢复**：消费者重启后自动处理未完成的消息

```go
// 消费者启动后会自动：
// 1. 检查PendingList
// 2. 抢夺超时消息
// 3. 处理并ACK消息
// 4. 然后处理新消息
```

### 3. 消息停止处理控制

#### 停止特定消息的处理
```go
// 停止消息处理（所有消费者都会跳过这个消息）
mq.StopMessageProcessing(messageID)

// 恢复消息处理
mq.ResumeMessageProcessing(messageID)
```

#### 使用场景
- 发现有问题的消息需要暂停处理
- 需要修复消息处理逻辑后再恢复
- 临时阻止某些消息的处理

### 4. Topic终止功能

#### 完全清空Topic
```go
// 终止整个topic，清空所有消息和消费者组
err := mq.TerminateTopic(ctx)
if err != nil {
    log.Printf("终止失败: %v", err)
}
```

#### 使用场景
- 需要清空所有积压的消息
- 重置消息队列状态
- 紧急停止所有消息处理

#### 注意事项
⚠️ **警告**：此操作不可逆，会删除：
- 所有未处理的消息
- 所有消费者组
- 所有Pending消息

## 🛠️ 管理工具

### Topic管理器

#### 查看Topic信息
```bash
# 使用Makefile
make topic-info

# 或直接运行
go run cmd/topic_manager.go -action=info -stream=my-stream
```

#### 终止Topic
```bash
# 使用Makefile
make topic-terminate

# 或直接运行
go run cmd/topic_manager.go -action=terminate -stream=my-stream
```

### Redis管理

```bash
# 连接Redis CLI
make redis-cli

# 查看Redis日志
make redis-logs

# 停止Redis服务
make redis-down
```

## 📊 监控和调试

### 查看Stream信息
```bash
# 在Redis CLI中
XINFO STREAM my-stream
XINFO GROUPS my-stream
XPENDING my-stream my-group
```

### 获取Topic信息
```go
info, err := mq.GetTopicInfo(ctx)
if err != nil {
    log.Printf("获取信息失败: %v", err)
    return
}

fmt.Printf("消息数量: %d\n", info.Length)
fmt.Printf("消费者组: %d\n", len(info.Groups))

for _, group := range info.Groups {
    fmt.Printf("组 %s: %d pending\n", group.Name, group.Pending)
    for _, consumer := range group.Consumers {
        fmt.Printf("  消费者 %s: %d pending\n", consumer.Name, consumer.Pending)
    }
}
```

## 🔧 自定义消息处理器

### 实现MessageHandler接口
```go
type MyHandler struct{}

func (h *MyHandler) GetMessageType() string {
    return "my-message-type"
}

func (h *MyHandler) Handle(ctx context.Context, msg *queue.Message) error {
    // 处理消息逻辑
    log.Printf("处理消息: %s", msg.ID)
    
    // 从消息数据中获取字段
    data := msg.Data["field"].(string)
    
    // 执行业务逻辑
    // ...
    
    return nil
}

// 注册处理器
mq.RegisterHandler(&MyHandler{})
```

## 🎯 最佳实践

### 1. 消息幂等性
确保消息处理器具有幂等性，因为消息可能被重复处理：

```go
func (h *MyHandler) Handle(ctx context.Context, msg *queue.Message) error {
    // 检查是否已处理过
    if isAlreadyProcessed(msg.ID) {
        return nil // 幂等处理
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

### 2. 错误处理
实现适当的错误处理和重试机制：

```go
func (h *MyHandler) Handle(ctx context.Context, msg *queue.Message) error {
    maxRetries := 3
    for i := 0; i < maxRetries; i++ {
        err := processMessage(msg)
        if err == nil {
            return nil
        }
        
        if isRetryableError(err) {
            time.Sleep(time.Second * time.Duration(i+1))
            continue
        }
        
        return err // 不可重试的错误
    }
    
    return fmt.Errorf("达到最大重试次数")
}
```

### 3. 优雅关闭
确保程序能够优雅关闭：

```go
// 监听中断信号
c := make(chan os.Signal, 1)
signal.Notify(c, os.Interrupt, syscall.SIGTERM)

go func() {
    <-c
    log.Println("收到停止信号，正在关闭...")
    mq.Stop() // 优雅停止
    os.Exit(0)
}()
```

### 4. 监控指标
添加监控指标来跟踪系统状态：

```go
// 定期获取topic信息
ticker := time.NewTicker(time.Minute)
go func() {
    for range ticker.C {
        info, err := mq.GetTopicInfo(ctx)
        if err != nil {
            continue
        }
        
        // 发送指标到监控系统
        metrics.Gauge("queue.length", float64(info.Length))
        metrics.Gauge("queue.groups", float64(len(info.Groups)))
    }
}()
```

## 🚨 故障排除

### 常见问题

1. **Redis连接失败**
   ```bash
   # 检查Redis是否运行
   make redis-up
   
   # 检查连接
   redis-cli ping
   ```

2. **消息处理缓慢**
   ```bash
   # 检查pending消息
   redis-cli XPENDING stream-name group-name
   
   # 增加消费者数量
   # 优化消息处理逻辑
   ```

3. **消息积压**
   ```bash
   # 查看消息数量
   make topic-info
   
   # 如果需要清空
   make topic-terminate
   ```

4. **消费者无法启动**
   - 检查Redis连接
   - 检查Stream和消费者组是否存在
   - 查看错误日志

### 调试技巧

1. **启用详细日志**
   ```go
   log.SetLevel(log.DebugLevel)
   ```

2. **使用Redis CLI监控**
   ```bash
   # 实时监控命令
   redis-cli MONITOR
   
   # 查看Stream信息
   redis-cli XINFO STREAM stream-name
   ```

3. **检查消息内容**
   ```bash
   # 读取最新消息
   redis-cli XREAD COUNT 1 STREAMS stream-name $
   
   # 读取所有消息
   redis-cli XRANGE stream-name - +
   ```

这个消息队列系统提供了完整的功能集，可以满足各种生产环境的需求！ 