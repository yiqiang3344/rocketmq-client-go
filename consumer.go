package rocketmq_client

import (
	"context"
	"errors"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"strings"
	"time"
)

type ConsumerOptionFunc func(options *ConsumerOptions)

func WithConsumerOptionAwaitDuration(AwaitDuration time.Duration) ConsumerOptionFunc {
	return func(o *ConsumerOptions) {
		o.AwaitDuration = AwaitDuration
	}
}

func WithConsumerOptionMaxMessageNum(MaxMessageNum int32) ConsumerOptionFunc {
	return func(o *ConsumerOptions) {
		o.MaxMessageNum = MaxMessageNum
	}
}

func WithConsumerOptionInvisibleDuration(InvisibleDuration time.Duration) ConsumerOptionFunc {
	return func(o *ConsumerOptions) {
		o.InvisibleDuration = InvisibleDuration
	}
}

func WithConsumerOptionSubExpressions(SubExpressions map[string]*rmq_client.FilterExpression) ConsumerOptionFunc {
	return func(o *ConsumerOptions) {
		o.SubExpressions = SubExpressions
	}
}

type ConsumerOptions struct {
	AwaitDuration     time.Duration                           //接收消息的超时时间，默认5秒，实际值为设置值+3秒
	MaxMessageNum     int32                                   //每次接收的消息数量，默认10
	InvisibleDuration time.Duration                           //接收到的消息的不可见时间，默认10秒
	SubExpressions    map[string]*rmq_client.FilterExpression //订阅表达式，必填，key为topic，简单消费类型只支持tag和sql匹配
}

type Consumer interface {
	Ack(ctx context.Context) error
	ChangeInvisibleDuration(invisibleDuration time.Duration) error
	ChangeInvisibleDurationAsync(invisibleDuration time.Duration)
}

type defaultConsumer struct {
	mv       *rmq_client.MessageView
	consumer rmq_client.SimpleConsumer
}

func (s defaultConsumer) Ack(ctx context.Context) error {
	return s.consumer.Ack(ctx, s.mv)
}

func (s defaultConsumer) ChangeInvisibleDuration(invisibleDuration time.Duration) error {
	return s.consumer.ChangeInvisibleDuration(s.mv, invisibleDuration)
}

func (s defaultConsumer) ChangeInvisibleDurationAsync(invisibleDuration time.Duration) {
	s.consumer.ChangeInvisibleDurationAsync(s.mv, invisibleDuration)
	return
}

// ConsumeFunc 消费方法
// 方法内消费成功时需要调用consumer.Ack()；
// 消费时间可能超过消费者MaxMessageNum设置的时间时，可调用consumer.ChangeInvisibleDuration()或consumer.ChangeInvisibleDurationAsync()方法调整消息消费超时时间；
type ConsumeFunc func(ctx context.Context, msg *rmq_client.MessageView, consumer Consumer) error

// SimpleConsume 简单消费类型消费
func SimpleConsume(ctx context.Context, cfg *Config, consumeFunc ConsumeFunc, oFunc ...ConsumerOptionFunc) (stopFunc func(), err error) {
	err = checkCfg(cfg)
	if err != nil {
		return
	}

	o := ConsumerOptions{
		AwaitDuration:     time.Second * 5,
		MaxMessageNum:     10,
		InvisibleDuration: time.Second * 10,
	}
	options := &o
	if len(oFunc) > 0 {
		for _, f := range oFunc {
			f(options)
		}
	}

	if len(options.SubExpressions) == 0 {
		err = errors.New("SubExpressions不能为空")
		debugLog(cfg, "消费者参数不合法:%v", err)
		return
	}

	if strings.Trim(cfg.ConsumerGroup, "") == "" {
		err = errors.New("ConsumerGroup不能为空")
		debugLog(cfg, "消费者参数不合法:%v", err)
		return
	}

	consumer, err := rmq_client.NewSimpleConsumer(
		getRmqCfg(cfg),
		rmq_client.WithAwaitDuration(options.AwaitDuration),
		rmq_client.WithSubscriptionExpressions(options.SubExpressions),
	)
	if err != nil {
		debugLog(cfg, "初始化消费者失败:%v", err)
		return nil, err
	}

	err = consumer.Start()
	if err != nil {
		debugLog(cfg, "消费者启动失败:%v", err)
		return nil, err
	}

	stopFunc = func() {
		err1 := consumer.GracefulStop()
		if err1 != nil {
			debugLog(cfg, "消费者注销失败:%v", err1)
			return
		}
		debugLog(cfg, "消费者注销成功")
	}

	go func() {
		for {
			mvs, err1 := consumer.Receive(ctx, options.MaxMessageNum, options.InvisibleDuration)
			if err1 != nil {
				if IsNoNewMessage(err1) {
					//无新消息，暂停一会儿再获取
					time.Sleep(time.Second)
					continue
				}
				debugLog(cfg, "获取消息失败:%v", err1)
			}
			for _, mv := range mvs {
				consumeFunc(ctx, mv, &defaultConsumer{
					mv:       mv,
					consumer: consumer,
				})
			}
		}
	}()
	return
}
