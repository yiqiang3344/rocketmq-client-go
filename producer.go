package rocketmq_client

import (
	"context"
	"errors"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
)

type Producer interface {
	Stop() error                                                                                            //注销消费者
	Send(ctx context.Context, topicType TopicType, msg Message) (resp []*rmq_client.SendReceipt, err error) //同步发送消息
	SendAsync(ctx context.Context, topicType TopicType, msg Message, dealFunc SendAsyncDealFunc) error      //异步发送消息
	SendTransaction(ctx context.Context, message Message, confirmFunc ConfirmFunc) error                    //发送事务消息
}

func GetProducer(cfg *Config, oFunc ...ProducerOptionFunc) (producer Producer, err error) {
	p, err := startProducer(cfg, oFunc...)
	if err != nil {
		return
	}
	producer = &defaultProducer{
		Cfg:      cfg,
		producer: p,
	}
	return
}

type defaultProducer struct {
	Cfg      *Config
	producer rmq_client.Producer
}

func (s *defaultProducer) debugLog(format string, args ...any) {
	debugLog(s.Cfg, format, args...)
}

// StopProducer 注销生产者
func (s *defaultProducer) Stop() error {
	err := stopProducer(s.Cfg, s.producer)
	if err != nil {
		return err
	}
	s.producer = nil
	return nil
}

// Send 同步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func (s *defaultProducer) Send(ctx context.Context, topicType TopicType, msg Message) (resp []*rmq_client.SendReceipt, err error) {
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
func (s *defaultProducer) SendAsync(ctx context.Context, topicType TopicType, msg Message, dealFunc SendAsyncDealFunc) (err error) {
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
func (s *defaultProducer) SendTransaction(ctx context.Context, message Message, confirmFunc ConfirmFunc) (err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	_, err = SendTransaction(ctx, s.Cfg, s.producer, message, confirmFunc)
	return
}

type ProducerOptions struct {
	Topics             []string                   //支持的主题列表，可选
	MaxAttempts        int32                      //重试次数，可选
	transactionChecker SendTransactionCheckerFunc //事务检查器，事务消息必填
}

func WithProducerOptionTopics(Topics ...string) ProducerOptionFunc {
	return func(o *ProducerOptions) {
		o.Topics = Topics
	}
}

type ProducerOptionFunc func(options *ProducerOptions)

func WithProducerOptionMaxAttempts(maxAttempts int32) ProducerOptionFunc {
	return func(o *ProducerOptions) {
		o.MaxAttempts = maxAttempts
	}
}

type SendTransactionCheckerFunc func(msg *rmq_client.MessageView) rmq_client.TransactionResolution

func WithProducerOptionTransactionChecker(transactionChecker SendTransactionCheckerFunc) ProducerOptionFunc {
	return func(o *ProducerOptions) {
		o.transactionChecker = transactionChecker
	}
}

func startProducer(cfg *Config, oFunc ...ProducerOptionFunc) (producer rmq_client.Producer, err error) {
	err = checkCfg(cfg)
	if err != nil {
		return
	}

	o := ProducerOptions{
		MaxAttempts: 3,
	}
	options := &o
	if len(oFunc) > 0 {
		for _, f := range oFunc {
			f(options)
		}
	}

	producer, err = rmq_client.NewProducer(
		getRmqCfg(cfg),
		rmq_client.WithTopics(options.Topics...),
		rmq_client.WithMaxAttempts(options.MaxAttempts),
		rmq_client.WithTransactionChecker(&rmq_client.TransactionChecker{
			Check: options.transactionChecker,
		}),
	)
	if err != nil {
		debugLog(cfg, "生产者初始化失败:%v", err)
		return
	}
	err = producer.Start()
	if err != nil {
		debugLog(cfg, "生产者启动失败:%v", err)
		return
	}
	return
}

func stopProducer(cfg *Config, producer rmq_client.Producer) error {
	err := producer.GracefulStop()
	if err != nil {
		debugLog(cfg, "生产者注销失败:%v", err)
		return err
	}
	debugLog(cfg, "生产者注销成功")
	return nil
}
