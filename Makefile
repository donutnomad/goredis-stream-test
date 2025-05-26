.PHONY: help build run test clean redis-up redis-down redis-logs redis-cli

# 默认目标
help:
	@echo "可用的命令:"
	@echo "  build          - 编译项目"
	@echo "  run            - 运行主程序"
	@echo "  test           - 运行简单测试"
	@echo "  test-pending   - 运行Pending消息处理测试"
	@echo "  test-terminate - 运行Topic终止功能测试"
	@echo "  test-errors    - 运行错误处理测试"
	@echo "  test-ack-errors- 运行ACK错误测试"
	@echo "  test-start-position - 运行开始位置测试"
	@echo "  demo-start-position - 运行开始位置演示"
	@echo "  test-cleanup   - 运行消息清理测试"
	@echo "  demo-cleanup   - 运行消息清理演示"
	@echo "  test-batch     - 运行批量处理测试"
	@echo "  demo-batch     - 运行批量处理演示"
	@echo "  test-concurrent-cleanup - 运行并发清理测试"
	@echo "  test-coordinated-cleanup - 运行协调清理测试"
	@echo "  test-auto-stop - 运行自动停止测试"
	@echo "  demo-auto-stop - 运行自动停止演示"
	@echo "  clean          - 清理编译文件"
	@echo "  redis-up       - 启动Redis服务"
	@echo "  redis-down     - 停止Redis服务"
	@echo "  redis-logs     - 查看Redis日志"
	@echo "  redis-cli      - 连接到Redis CLI"
	@echo "  topic-info     - 查看Topic信息"
	@echo "  topic-terminate- 终止Topic"

# 编译项目
build:
	@echo "编译项目..."
	go mod tidy
	go build -o gostream .

# 运行主程序
run: build
	@echo "运行主程序..."
	./gostream

# 运行简单测试
test: build
	@echo "运行简单测试..."
	go run examples/simple_test.go

# 运行Pending消息测试
test-pending: build
	@echo "运行Pending消息处理测试..."
	go run examples/pending_test.go

# 运行Topic终止测试
test-terminate: build
	@echo "运行Topic终止功能测试..."
	go run examples/terminate_topic_test.go

# 运行开始位置演示
demo-start-position: build
	@echo "运行开始位置演示..."
	go run examples/start_position_demo.go

# 运行消息清理演示
demo-cleanup: build
	@echo "运行消息清理演示..."
	go run examples/cleanup_demo.go

# 运行错误处理测试
test-errors: build
	@echo "运行错误处理测试..."
	cd tests && go test -v -run TestErrorHandling

# 运行ACK错误测试
test-ack-errors: build
	@echo "运行ACK错误测试..."
	cd tests && go test -v -run TestACKError

# 运行开始位置测试
test-start-position: build
	@echo "运行开始位置测试..."
	cd tests && go test -v -run TestStart

# 运行消息清理测试
test-cleanup: build
	@echo "运行消息清理测试..."
	cd tests && go test -v -run TestCleanup

# 运行批量处理测试
test-batch: build
	@echo "运行批量处理测试..."
	cd tests && go test -v -run TestBatch

# 运行批量处理演示
demo-batch: build
	@echo "运行批量处理演示..."
	go run examples/batch_demo.go

# 运行并发清理测试
test-concurrent-cleanup: build
	@echo "运行并发清理测试..."
	cd tests && go test -v -run TestConcurrent

# 运行协调清理测试
test-coordinated-cleanup: build
	@echo "运行协调清理测试..."
	cd tests && go test -v -run TestCoordinated

# 运行自动停止测试
test-auto-stop: build
	@echo "运行自动停止测试..."
	cd tests && go test -v -run TestAutoStop

# 运行自动停止演示
demo-auto-stop: build
	@echo "运行自动停止演示..."
	go run examples/auto_stop_demo.go

# 清理编译文件
clean:
	@echo "清理编译文件..."
	rm -f gostream

# 启动Redis服务
redis-up:
	@echo "启动Redis服务..."
	docker-compose up -d redis
	@echo "等待Redis启动..."
	sleep 3
	@echo "Redis已启动在 localhost:6379"

# 停止Redis服务
redis-down:
	@echo "停止Redis服务..."
	docker-compose down

# 查看Redis日志
redis-logs:
	docker-compose logs -f redis

# 连接到Redis CLI
redis-cli:
	docker exec -it gostream-redis redis-cli

# 查看Stream信息
redis-info:
	@echo "查看Stream信息..."
	docker exec -it gostream-redis redis-cli XINFO STREAM task-stream || echo "Stream不存在"
	docker exec -it gostream-redis redis-cli XINFO GROUPS task-stream || echo "消费者组不存在"

# Topic管理工具
topic-info:
	@echo "查看Topic信息..."
	@read -p "请输入Stream名称: " stream; \
	go run cmd/topic_manager.go -action=info -stream=$$stream

topic-terminate:
	@echo "终止Topic..."
	@read -p "请输入Stream名称: " stream; \
	go run cmd/topic_manager.go -action=terminate -stream=$$stream 