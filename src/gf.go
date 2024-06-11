package rocketmq_client

import (
	"context"
	"errors"
	"fmt"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/net/gtrace"
	"github.com/gogf/gf/v2/util/gconv"
	"go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
)

// GetGfClient 获取mq客户端
func GetGfClient(cfg *Config) (client Client, err error) {
	c, err := GetClient(cfg)
	if err != nil {
		return
	}
	client = &defaultGfClient{
		defaultClient: c.(*defaultClient),
	}
	return
}

type defaultGfClient struct {
	*defaultClient
}

func addSendTrace(ctx context.Context, msg Message) func(resp []*rmq_client.SendReceipt, err error) {
	_, span := gtrace.NewSpan(ctx, "rocketmqSend")
	span.SetAttributes(
		attribute.String("Body", msg.Body),
		attribute.String("Topic", msg.Topic),
		attribute.String("Tag", msg.Tag),
		attribute.String("MessageGroup", msg.MessageGroup),
		attribute.StringSlice("Keys", msg.Keys),
		attribute.String("Properties", gmap.NewStrStrMapFrom(msg.Properties).String()),
		attribute.String("DeliveryTimestamp", gconv.String(msg.DeliveryTimestamp)),
	)
	return func(resp []*rmq_client.SendReceipt, err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(
				attribute.String("SendReceipt", fmt.Sprintf("%+v", resp)),
			)
		} else {
			span.SetStatus(codes.Ok, "success")
		}
		span.End()
	}
}

// Send 同步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func (s *defaultGfClient) Send(ctx context.Context, topicType TopicType, msg Message) (resp []*rmq_client.SendReceipt, err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}

	//记录链路追踪span
	endFunc := addSendTrace(ctx, msg)
	resp, err = Send(ctx, s.Cfg, s.producer, topicType, msg)
	endFunc(resp, err)
	return
}

// SendAsync 异步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func (s *defaultGfClient) SendAsync(ctx context.Context, topicType TopicType, msg Message, dealFunc SendAsyncDealFunc) (err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	//记录链路追踪span
	endFunc := addSendTrace(ctx, msg)
	err = SendAsync(ctx, s.Cfg, s.producer, topicType, msg, func(ctx context.Context, msg Message, resp []*rmq_client.SendReceipt, err error) {
		endFunc(resp, err)
		dealFunc(ctx, msg, resp, err)
	})
	return
}

// SendTransaction 发送事务消息
// 注意：事务消息的生产者不能和其他类型消息的生产者共用
func (s *defaultGfClient) SendTransaction(ctx context.Context, msg Message, confirmFunc ConfirmFunc) (err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	//记录链路追踪span
	endFunc := addSendTrace(ctx, msg)
	resp, err := SendTransaction(ctx, s.Cfg, s.producer, msg, confirmFunc)
	endFunc(resp, err)
	return
}

// SimpleConsume 简单消费类型消费
func (s *defaultGfClient) SimpleConsume(ctx context.Context, consumeFunc ConsumeFunc, oFunc ...ConsumerOptionFunc) (stopFunc func(), err error) {
	return SimpleConsume(ctx, s.Cfg, func(ctx context.Context, msg *rmq_client.MessageView, consumer Consumer) {
		ctx, span := gtrace.NewSpan(ctx, "rocketmqSend")
		span.SetAttributes(
			attribute.String("Body", string(msg.GetBody())),
			attribute.String("Topic", msg.GetTopic()),
			attribute.String("Tag", gconv.String(msg.GetTag())),
			attribute.String("MessageGroup", gconv.String(msg.GetMessageGroup())),
			attribute.StringSlice("Keys", msg.GetKeys()),
			attribute.String("Properties", gmap.NewStrStrMapFrom(msg.GetProperties()).String()),
			attribute.String("DeliveryTimestamp", gconv.String(msg.GetDeliveryTimestamp())),
		)

		consumeFunc(ctx, msg, consumer)

		span.End()
	}, oFunc...)
}
