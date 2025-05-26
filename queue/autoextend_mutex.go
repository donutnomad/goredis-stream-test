package queue

import (
	"context"
	"log"
	"time"

	"github.com/go-redsync/redsync/v4"
)

// AutoExtendMutex 自动续期的互斥锁
type AutoExtendMutex struct {
	mutex    *redsync.Mutex
	cancel   context.CancelFunc
	interval time.Duration
}

// NewAutoExtendMutex 创建一个新的自动续期互斥锁
func NewAutoExtendMutex(mutex *redsync.Mutex, interval time.Duration) *AutoExtendMutex {
	return &AutoExtendMutex{
		mutex:    mutex,
		interval: interval,
	}
}

// Lock 获取锁
func (m *AutoExtendMutex) Lock() error {
	return m.LockContext(context.Background())
}

// LockContext 使用上下文获取锁
func (m *AutoExtendMutex) LockContext(ctx context.Context) error {
	autoExtendCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	if err := m.mutex.LockContext(ctx); err != nil {
		m.cancel()
		return err
	}
	if m.interval > 0 {
		go m.autoExtend(autoExtendCtx)
	}
	return nil
}

// Unlock 释放锁
func (m *AutoExtendMutex) Unlock() (bool, error) {
	if m.cancel != nil {
		m.cancel()
	}
	return m.mutex.Unlock()
}

// autoExtend 自动续期
func (m *AutoExtendMutex) autoExtend(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.extend(ctx, 3) {
				return
			}
		}
	}
}

// extend 尝试延长锁的过期时间
func (m *AutoExtendMutex) extend(ctx context.Context, maxRetries uint) bool {
	var success bool
	var err error

	// 简单重试逻辑
	for i := uint(0); i < maxRetries; i++ {
		success, err = m.mutex.ExtendContext(ctx)
		if err == nil && success {
			return true
		}
		if i < maxRetries-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if err != nil || !success {
		log.Printf("续期失败，重试次数耗尽")
	}

	return false
}
