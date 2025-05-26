package handlers

import (
	"context"
	"fmt"
	"log"
	"time"

	"goStream/queue"
)

// EmailHandler 邮件处理器
type EmailHandler struct{}

// NewEmailHandler 创建邮件处理器
func NewEmailHandler() *EmailHandler {
	return &EmailHandler{}
}

// GetMessageType 返回处理的消息类型
func (h *EmailHandler) GetMessageType() string {
	return "email"
}

// Handle 处理邮件消息
func (h *EmailHandler) Handle(ctx context.Context, msg *queue.Message) error {
	log.Printf("开始处理邮件消息: %s", msg.ID)

	// 从消息数据中获取邮件信息
	to, ok := msg.Data["to"].(string)
	if !ok {
		return fmt.Errorf("缺少收件人信息")
	}

	subject, ok := msg.Data["subject"].(string)
	if !ok {
		return fmt.Errorf("缺少邮件主题")
	}

	body, ok := msg.Data["body"].(string)
	if !ok {
		return fmt.Errorf("缺少邮件内容")
	}

	// 模拟发送邮件
	log.Printf("发送邮件到: %s, 主题: %s", to, subject)

	// 模拟处理时间
	time.Sleep(time.Second * 2)

	// 检查是否有特殊的失败标记（用于测试）
	if body == "FAIL" {
		return fmt.Errorf("模拟邮件发送失败")
	}

	log.Printf("邮件发送成功: %s", msg.ID)
	return nil
}
