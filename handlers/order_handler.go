package handlers

import (
	"context"
	"fmt"
	"log"
	"time"

	"goStream/queue"
)

// OrderHandler 订单处理器
type OrderHandler struct{}

// NewOrderHandler 创建订单处理器
func NewOrderHandler() *OrderHandler {
	return &OrderHandler{}
}

// GetMessageType 返回处理的消息类型
func (h *OrderHandler) GetMessageType() string {
	return "order"
}

// Handle 处理订单消息
func (h *OrderHandler) Handle(ctx context.Context, msg *queue.Message) error {
	log.Printf("开始处理订单消息: %s", msg.ID)

	// 从消息数据中获取订单信息
	orderID, ok := msg.Data["order_id"].(string)
	if !ok {
		return fmt.Errorf("缺少订单ID")
	}

	userID, ok := msg.Data["user_id"].(string)
	if !ok {
		return fmt.Errorf("缺少用户ID")
	}

	amount, ok := msg.Data["amount"].(float64)
	if !ok {
		return fmt.Errorf("缺少订单金额")
	}

	// 模拟处理订单
	log.Printf("处理订单: %s, 用户: %s, 金额: %.2f", orderID, userID, amount)

	// 模拟处理时间
	time.Sleep(time.Second * 3)

	// 检查是否有特殊的失败标记（用于测试）
	if orderID == "FAIL" {
		return fmt.Errorf("模拟订单处理失败")
	}

	log.Printf("订单处理成功: %s", msg.ID)
	return nil
}
