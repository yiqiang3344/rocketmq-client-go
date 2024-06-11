package rocketmq_client

import (
	"context"
	"errors"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"os"
	"strings"
)

type Client interface {
	StartProducer(ctx context.Context, oFunc ...ProducerOptionFunc) error                                                //启动生产者
	StopProducer() error                                                                                                 //注销消费者
	Send(ctx context.Context, topicType TopicType, msg Message) (resp []*rmq_client.SendReceipt, err error)              //同步发送消息
	SendAsync(ctx context.Context, topicType TopicType, msg Message, dealFunc SendAsyncDealFunc) error                   //异步发送消息
	SendTransaction(ctx context.Context, message Message, confirmFunc ConfirmFunc) error                                 //发送事务消息
	SimpleConsume(ctx context.Context, consumeFuc ConsumeFunc, oFunc ...ConsumerOptionFunc) (stopFunc func(), err error) //简单模式消费消息
}

// GetClient 获取mq客户端
func GetClient(cfg *Config) (client Client, err error) {
	if cfg.LogStdout {
		os.Setenv("mq.consoleAppender.enabled", "true")
	} else {
		os.Setenv("mq.consoleAppender.enabled", "false")
	}
	os.Setenv("rocketmq.client.logRoot", cfg.LogPath)
	rmq_client.ResetLogger()

	if strings.Trim(cfg.Endpoint, "") == "" {
		err = errors.New("Endpoint不能为空")
		return
	}

	if strings.Trim(cfg.NameSpace, "") == "" {
		err = errors.New("NameSpace不能为空")
		return
	}

	client = &defaultClient{
		Cfg: cfg,
	}
	return
}

type defaultClient struct {
	Cfg      *Config
	producer rmq_client.Producer
}

func (s *defaultClient) debugLog(format string, args ...any) {
	debugLog(s.Cfg, format, args...)
}

// StartProducer 启动生产者
func (s *defaultClient) StartProducer(ctx context.Context, oFunc ...ProducerOptionFunc) (err error) {
	s.producer, err = StartProducer(ctx, s.Cfg, oFunc...)
	return nil
}

// StopProducer 注销生产者
func (s *defaultClient) StopProducer() error {
	err := StopProducer(s.Cfg, s.producer)
	if err != nil {
		return err
	}
	s.producer = nil
	return nil
}

// Send 同步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func (s *defaultClient) Send(ctx context.Context, topicType TopicType, msg Message) (resp []*rmq_client.SendReceipt, err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}

	resp, err = Send(ctx, s.Cfg, s.producer, topicType, msg)
	return
}

// SendAsync 异步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func (s *defaultClient) SendAsync(ctx context.Context, topicType TopicType, msg Message, dealFunc SendAsyncDealFunc) (err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	err = SendAsync(ctx, s.Cfg, s.producer, topicType, msg, dealFunc)
	return
}

// SendTransaction 发送事务消息
// 注意：事务消息的生产者不能和其他类型消息的生产者共用
func (s *defaultClient) SendTransaction(ctx context.Context, message Message, confirmFunc ConfirmFunc) (err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	_, err = SendTransaction(ctx, s.Cfg, s.producer, message, confirmFunc)
	return
}

// SimpleConsume 简单消费类型消费
func (s *defaultClient) SimpleConsume(ctx context.Context, consumeFunc ConsumeFunc, oFunc ...ConsumerOptionFunc) (stopFunc func(), err error) {
	return SimpleConsume(ctx, s.Cfg, consumeFunc, oFunc...)
}
