package rocketmq_client

import (
	"context"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
)

type ProducerOptions struct {
	Topics             []string                   //支持的主题列表，可选
	MaxAttempts        int32                      //重试次数，可选
	transactionChecker SendTransactionCheckerFunc //事务检查器，事务消息必填
}

func WithProducerOptionTopics(Topics []string) ProducerOptionFunc {
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

// StartProducer 启动生产者
func StartProducer(ctx context.Context, cfg *Config, oFunc ...ProducerOptionFunc) (producer rmq_client.Producer, err error) {
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

// StopProducer 注销生产者
func StopProducer(cfg *Config, producer rmq_client.Producer) error {
	err := producer.GracefulStop()
	if err != nil {
		debugLog(cfg, "生产者注销失败:%v", err)
		return err
	}
	debugLog(cfg, "生产者注销成功")
	return nil
}
