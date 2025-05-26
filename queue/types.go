package queue

import "time"

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
