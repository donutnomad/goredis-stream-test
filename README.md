# Go Redis Stream 消息队列系统

这是一个基于 Redis Stream 和 go-redis 库构建的完美消息队列系统，具有以下特性：

## 🚀 核心特性

### 1. Pending List 处理机制
- **优先处理 Pending 消息**：消费者启动时首先处理 PendingList 中的消息
- **消息抢夺机制**：当消费者在设计时间内未处理完消息，其他消费者可以抢夺过去
- **自动 ACK**：处理完成后自动确认消息

### 2. 消息停止处理控制
- **动态停止**：可以方便地停止特定消息的处理
- **全局控制**：停止后所有消费者都不会处理该消息
- **恢复处理**：可以重新启用被停止的消息处理

### 3. Topic终止功能
- **完全清空**：可以终止整个topic，清空所有未消费的消息
- **消费者组清理**：自动删除所有相关的消费者组
- **即时生效**：正在消费的消费者会立即停止
- **可重建**：终止后可以重新创建同名topic

### 4. 消费者开始位置配置
- **从最新位置开始**：只消费启动后的新消息（默认）
- **从最早位置开始**：消费所有历史消息和新消息
- **从指定ID开始**：从指定的消息ID开始消费
- **灵活配置**：创建消费者时可以灵活选择开始位置

### 5. 消息清理机制
- **自动清理**：定期清理已被所有消费者组处理的消息
- **手动清理**：按需清理，完全控制清理时机
- **安全机制**：只清理被所有消费者组确认且足够老的消息
- **批量处理**：避免一次性删除太多消息影响性能

### 6. 批量消息处理
- **批量处理器**：支持一次处理多条消息，提高吞吐量
- **智能分批**：自动按配置的批量大小分批处理
- **超时机制**：不足一批时等待超时后处理现有消息
- **混合模式**：同时支持批量和单条处理器
- **向后兼容**：现有单条处理器无需修改即可使用

### 7. 自动停止机制
- **智能检测**：自动检测topic或消费者组被删除
- **优雅停止**：消费者自动停止，避免无效重试
- **外部监听**：提供Done()通道供外部监听停止事件
- **状态区分**：区分自动停止和手动停止

### 8. 高可用性设计
- **多消费者支持**：支持多个消费者并发处理
- **消息竞争处理**：多个消费者可以竞争处理消息
- **故障恢复**：消费者重启后自动处理未完成的消息

## 📁 项目结构

```
goStream/
├── main.go                 # 主程序，演示完整功能
├── go.mod                  # Go 模块文件
├── queue/
│   └── message_queue.go    # 消息队列核心实现
├── handlers/
│   ├── email_handler.go    # 邮件处理器示例
│   └── order_handler.go    # 订单处理器示例
└── README.md              # 项目说明文档
```

## 🛠️ 安装和运行

### 前置条件
1. Go 1.24+
2. Redis 服务器

### 安装依赖
```bash
go mod tidy
```

### 启动 Redis
```bash
# 使用 Docker 启动 Redis
docker run -d -p 6379:6379 redis:latest

# 或者使用本地 Redis
redis-server
```

### 运行程序

#### 快速开始
```bash
# 启动Redis服务
make redis-up

# 运行主程序（完整演示）
make run

# 或者运行简单测试
make test

# 或者运行Pending消息处理演示
make test-pending

# 或者运行Topic终止功能演示
make test-terminate

# 或者运行开始位置演示
make demo-start-position

# 或者运行消息清理演示
make demo-cleanup

# 或者运行批量处理演示
make demo-batch

# 或者运行自动停止演示
make demo-auto-stop
```

#### 手动运行
```bash
# 编译项目
make build

# 运行主程序
./gostream

# 或者直接运行
go run main.go
```

## 💡 核心组件说明

### MessageQueue 核心类
```go
type MessageQueue struct {
    client       *redis.Client
    streamName   string
    groupName    string
    consumerName string
    handlers     map[string]MessageHandler
    stopChan     chan struct{}
    wg           sync.WaitGroup
    mu           sync.RWMutex
    stopped      bool
    
    // 消息停止处理的控制
    stoppedMessages map[string]bool
    stopMsgMu       sync.RWMutex
}
```

### 关键方法

#### 1. 启动消息队列
```go
func (mq *MessageQueue) Start(ctx context.Context) error
```
- 创建消费者组
- 启动 Pending 消息处理 goroutine
- 启动新消息处理 goroutine

#### 2. 处理 Pending 消息
```go
func (mq *MessageQueue) processPendingMessages(ctx context.Context)
```
- 获取 PendingList 中的消息
- 使用 XCLAIM 抢夺超时消息
- 优先处理完所有 Pending 消息

#### 3. 消息处理控制
```go
func (mq *MessageQueue) StopMessageProcessing(messageID string)
func (mq *MessageQueue) ResumeMessageProcessing(messageID string)
```
- 动态停止/恢复特定消息的处理
- 线程安全的控制机制

## 🔧 使用示例

### 1. 创建消息队列

#### 基本创建（默认从最新位置开始）
```go
rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

mq := queue.NewMessageQueue(rdb, "task-stream", "task-group", "consumer-1")
```

#### 指定开始位置创建
```go
// 从最早位置开始消费（处理所有历史消息）
mq := queue.NewMessageQueueWithStartPosition(rdb, "task-stream", "task-group", "consumer-1", queue.StartFromEarliest)

// 从最新位置开始消费（默认行为）
mq := queue.NewMessageQueueWithStartPosition(rdb, "task-stream", "task-group", "consumer-1", queue.StartFromLatest)

// 从指定消息ID开始消费
mq := queue.NewMessageQueueFromSpecificID(rdb, "task-stream", "task-group", "consumer-1", "1234567890-0")
```

#### 配置消息清理策略
```go
// 启用自动清理
cleanupPolicy := &queue.CleanupPolicy{
    EnableAutoCleanup: true,
    CleanupInterval:   time.Minute * 5,  // 5分钟检查一次
    MaxStreamLength:   10000,            // 超过10000条消息时清理
    MinRetentionTime:  time.Hour * 1,    // 消息至少保留1小时
    BatchSize:         100,              // 每批清理100条消息
}
mq.SetCleanupPolicy(cleanupPolicy)
```

#### 配置批量处理
```go
// 启用批量处理
batchConfig := &queue.BatchConfig{
    EnableBatch:  true,
    BatchSize:    10,                   // 默认批量大小
    BatchTimeout: time.Millisecond * 500, // 500ms超时
    MaxWaitTime:  time.Second * 2,      // 最大等待2秒
}
mq.SetBatchConfig(batchConfig)
```

### 2. 注册消息处理器

#### 单条消息处理器
```go
// 实现 MessageHandler 接口
type EmailHandler struct{}

func (h *EmailHandler) GetMessageType() string {
    return "email"
}

func (h *EmailHandler) Handle(ctx context.Context, msg *queue.Message) error {
    // 处理邮件逻辑
    return nil
}

// 注册处理器
mq.RegisterHandler(&EmailHandler{})
```

#### 批量消息处理器
```go
// 实现 BatchMessageHandler 接口
type EmailBatchHandler struct{}

func (h *EmailBatchHandler) GetMessageType() string {
    return "email"
}

func (h *EmailBatchHandler) GetBatchSize() int {
    return 5 // 每批处理5封邮件，返回0使用全局配置
}

func (h *EmailBatchHandler) HandleBatch(ctx context.Context, messages []*queue.Message) error {
    // 批量处理邮件逻辑
    fmt.Printf("批量发送 %d 封邮件\n", len(messages))
    return nil
}

// 注册批量处理器
mq.RegisterBatchHandler(&EmailBatchHandler{})
```

### 3. 启动消费者
```go
ctx := context.Background()
err := mq.Start(ctx)
if err != nil {
    log.Fatal(err)
}
```

### 4. 发布消息
```go
messageID, err := mq.PublishMessage(ctx, "email", map[string]interface{}{
    "to":      "user@example.com",
    "subject": "欢迎注册",
    "body":    "欢迎您注册我们的服务！",
}, map[string]string{
    "priority": "high",
})
```

### 5. 控制消息处理
```go
// 停止特定消息的处理
mq.StopMessageProcessing(messageID)

// 恢复消息处理
mq.ResumeMessageProcessing(messageID)

// 终止整个topic（清空所有消息和消费者组）
err := mq.TerminateTopic(ctx)

// 获取topic信息
info, err := mq.GetTopicInfo(ctx)

// 手动清理消息
cleaned, err := mq.CleanupMessages(ctx)
```

### 6. 监听自动停止
```go
// 启动消费者
consumer := queue.NewMessageQueue(rdb, "my-stream", "my-group", "consumer-1")
consumer.Start(ctx)

// 监听自动停止事件
go func() {
    <-consumer.Done()
    if consumer.IsAutoStopped() {
        log.Println("消费者因topic删除而自动停止")
        // 执行清理逻辑
    } else {
        log.Println("消费者手动停止")
    }
}()

// 手动停止消费者
consumer.Stop()
```

## 🎯 核心工作流程

### 消费者启动流程
1. **创建消费者组**：如果不存在则创建新的消费者组
2. **处理 Pending 消息**：
   - 查询 PendingList
   - 使用 XCLAIM 抢夺超时消息
   - 处理并 ACK 消息
3. **处理新消息**：
   - 使用 XREADGROUP 读取新消息
   - 处理并 ACK 消息

### 消息处理流程
1. **消息解析**：从 Redis Stream 消息中解析出结构化数据
2. **停止检查**：检查消息是否被标记为停止处理
3. **处理器查找**：根据消息类型找到对应的处理器
4. **消息处理**：调用处理器处理消息
5. **消息确认**：处理成功后 ACK 消息

### 消息抢夺机制
- 使用 Redis Stream 的 XCLAIM 命令
- 当消息在 PendingList 中超过一定时间未被处理时
- 其他消费者可以抢夺这些消息进行处理
- 确保消息不会因为消费者故障而丢失

## 🔍 监控和调试

### 查看 Stream 信息
```bash
# 查看 Stream 基本信息
redis-cli XINFO STREAM task-stream

# 查看消费者组信息
redis-cli XINFO GROUPS task-stream

# 查看 Pending 消息
redis-cli XPENDING task-stream task-group
```

### 日志输出
程序会输出详细的日志信息，包括：
- 消息发布日志
- 消息处理日志
- Pending 消息处理日志
- 错误和异常日志

## 🚨 注意事项

1. **Redis 版本**：需要 Redis 5.0+ 支持 Stream 功能
2. **消息幂等性**：确保消息处理器具有幂等性
3. **错误处理**：实现适当的错误处理和重试机制
4. **资源清理**：程序退出时正确关闭消息队列

## 🔮 扩展功能

### 可以添加的功能
1. **死信队列**：处理失败的消息
2. **消息重试**：自动重试失败的消息
3. **消息优先级**：支持消息优先级处理
4. **监控指标**：添加 Prometheus 监控
5. **配置管理**：支持配置文件和环境变量

这个消息队列系统提供了一个完整的、生产就绪的解决方案，可以直接用于实际项目中。

## 🧹 消息清理

### 自动清理已处理消息

当消息被所有消费者组都处理完成后，系统可以自动清理这些消息，防止Redis Stream无限增长：

1. **安全检查**：只清理被所有消费者组确认且足够老的消息
2. **自动清理**：定期检查并清理，保持Stream在合理大小
3. **手动清理**：按需清理，完全控制清理时机
4. **批量处理**：避免一次性删除太多消息影响性能

### 测试消息清理

```bash
# 运行消息清理测试
make test-cleanup

# 运行消息清理演示
make demo-cleanup
```

详细的消息清理说明请参考：[MESSAGE_CLEANUP.md](MESSAGE_CLEANUP.md)

## 🚀 批量消息处理

### 高性能批量处理

当需要处理大量消息时，批量处理可以显著提高吞吐量：

1. **减少网络开销**：一次处理多条消息
2. **提高处理效率**：批量API调用（如批量邮件发送）
3. **智能分批**：自动按配置大小分批
4. **超时保护**：避免消息积压

### 测试批量处理

```bash
# 运行批量处理测试
make test-batch

# 运行批量处理演示
make demo-batch
```

### 批量处理特性

- **灵活配置**：可配置批量大小、超时时间
- **类型分组**：相同类型的消息自动分组批量处理
- **回退机制**：没有批量处理器时自动回退到单条处理
- **混合支持**：同一消费者可同时注册批量和单条处理器

## 🚨 错误处理

### Topic删除时的错误处理

当消费者正在处理消息时，如果topic被删除，系统会自动：

1. **检测错误**：识别`NOGROUP`、`ERR no such key`等错误模式
2. **优雅停止**：停止消费者避免无效操作  
3. **完成处理**：让正在处理的消息完成
4. **详细日志**：记录所有错误便于调试

### 测试错误处理

```bash
# 运行错误处理测试
make test-errors
```

详细的错误处理说明请参考：[ERROR_HANDLING.md](ERROR_HANDLING.md)

## 🛑 自动停止功能

### 智能的消费者自动停止

当topic被删除或消费者组被删除时，消费者会自动检测并优雅停止：

1. **智能检测**：自动检测Redis Stream或消费者组被删除
2. **优雅停止**：消费者自动停止，避免无效的重试和错误日志
3. **外部监听**：提供Done()通道供外部监听停止事件
4. **状态区分**：通过IsAutoStopped()区分自动停止和手动停止

### 使用场景

- **微服务架构**：在容器化环境中实现优雅的服务关闭
- **动态扩缩容**：当topic被删除时，相关消费者自动停止
- **错误处理**：避免因topic删除导致的无效重试
- **资源管理**：自动释放不再需要的消费者资源

### 测试自动停止

```bash
# 运行自动停止测试
make test-auto-stop

# 运行自动停止演示
make demo-auto-stop
```

### 使用示例

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
        // 执行清理逻辑，如释放资源、更新状态等
    } else {
        log.Println("ℹ️  消费者手动停止")
    }
}()

// 在其他地方删除topic
terminator := queue.NewMessageQueue(rdb, "my-stream", "my-group", "terminator")
err = terminator.TerminateTopic(ctx)
// 此时消费者会自动停止
``` 