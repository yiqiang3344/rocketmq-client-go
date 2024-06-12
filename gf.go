package rocketmq_client

import (
	"context"
	"encoding/json"
	"errors"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/net/gtrace"
	"github.com/gogf/gf/v2/util/gconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"time"
)

func GetGfProducer(cfg *Config, oFunc ...ProducerOptionFunc) (producer Producer, err error) {
	p, err := GetProducer(cfg, oFunc...)
	if err != nil {
		return
	}
	producer = &defaultGfProducer{
		p.(*defaultProducer),
	}
	return
}

type defaultGfProducer struct {
	*defaultProducer
}

// Send 同步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func (s *defaultGfProducer) Send(ctx context.Context, topicType TopicType, msg Message) (resp []*rmq_client.SendReceipt, err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	//记录链路追踪span
	ctx, endFunc := addSendTrace(ctx, &msg)
	resp, err = Send(ctx, s.Cfg, s.producer, topicType, msg)
	endFunc(resp, err)
	return
}

// SendAsync 异步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func (s *defaultGfProducer) SendAsync(ctx context.Context, topicType TopicType, msg Message, dealFunc SendAsyncDealFunc) (err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	//记录链路追踪span
	ctx, endFunc := addSendTrace(ctx, &msg)
	err = SendAsync(ctx, s.Cfg, s.producer, topicType, msg, func(ctx context.Context, msg Message, resp []*rmq_client.SendReceipt, err error) {
		endFunc(resp, err)
		dealFunc(ctx, msg, resp, err)
	})
	return
}

// SendTransaction 发送事务消息
// 注意：事务消息的生产者不能和其他类型消息的生产者共用
func (s *defaultGfProducer) SendTransaction(ctx context.Context, msg Message, confirmFunc ConfirmFunc) (err error) {
	if s.producer == nil {
		err = errors.New("请先初始化生产者")
		s.debugLog("消息发送失败:%v", err)
		return
	}
	//记录链路追踪span
	ctx, endFunc := addSendTrace(ctx, &msg)
	resp, err := SendTransaction(ctx, s.Cfg, s.producer, msg, confirmFunc)
	endFunc(resp, err)
	return
}

func addSendTrace(ctx context.Context, msg *Message) (context.Context, func(resp []*rmq_client.SendReceipt, err error)) {
	ctx, span := gtrace.NewSpan(ctx, "rocketmqSend")

	//给消息设置链路信息
	spanContext := trace.SpanContextFromContext(ctx)
	if msg.Properties == nil {
		msg.Properties = map[string]string{}
	}
	msg.Properties["traceId"] = spanContext.TraceID().String()
	msg.Properties["spanId"] = spanContext.SpanID().String()

	span.SetAttributes(
		attribute.String("Body", msg.Body),
		attribute.String("Topic", msg.Topic),
		attribute.String("Tag", msg.Tag),
		attribute.String("MessageGroup", msg.MessageGroup),
		attribute.StringSlice("Keys", msg.Keys),
		attribute.String("Properties", gmap.NewStrStrMapFrom(msg.Properties).String()),
		attribute.String("DeliveryTimestamp", gconv.String(msg.DeliveryTimestamp)),
	)
	return ctx, func(resp []*rmq_client.SendReceipt, err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		} else {
			ret, _ := json.Marshal(resp)
			span.SetAttributes(
				attribute.String("SendReceipt", string(ret)),
			)
			span.SetStatus(codes.Ok, "success")
		}
		span.SetAttributes(attribute.String("endTime", time.Now().Format("2006-01-02 15:04:05.999")))
		span.End()
	}
}

// SimpleConsume4Gf gf版简单消费类型消费
func SimpleConsume4Gf(ctx context.Context, cfg *Config, consumeFunc ConsumeFunc, oFunc ...ConsumerOptionFunc) (stopFunc func(), err error) {
	return SimpleConsume(ctx, cfg, func(ctx context.Context, msg *rmq_client.MessageView, consumer Consumer) error {
		var (
			err  error
			span *gtrace.Span
		)
		if v, ok := msg.GetProperties()["traceId"]; ok {
			var (
				spanContext trace.SpanContextConfig
				errTranceId error
				errSpanId   error
				spanIdStr   = msg.GetProperties()["spanId"]
			)
			spanContext.TraceID, errTranceId = trace.TraceIDFromHex(v)
			spanContext.SpanID, errSpanId = trace.SpanIDFromHex(spanIdStr)
			if errTranceId != nil || errSpanId != nil {
				debugLog(cfg, "span context failed traceId[%s] spanId[%s] TraceError:%v spanError:%v", v, spanIdStr, errTranceId, errSpanId)
			} else {
				ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(spanContext))
				ctx, span = gtrace.NewSpan(ctx, "rocketmqConsume")
				span.SetAttributes(
					attribute.String("Body", string(msg.GetBody())),
					attribute.String("Topic", msg.GetTopic()),
					attribute.String("Tag", gconv.String(msg.GetTag())),
					attribute.String("MessageGroup", gconv.String(msg.GetMessageGroup())),
					attribute.StringSlice("Keys", msg.GetKeys()),
					attribute.String("Properties", gmap.NewStrStrMapFrom(msg.GetProperties()).String()),
					attribute.String("DeliveryTimestamp", gconv.String(msg.GetDeliveryTimestamp())),
				)
			}
		} else {
			debugLog(cfg, "message[%s]无traceInfo:%+v", msg.GetMessageId(), msg.GetProperties())
		}

		err = consumeFunc(ctx, msg, consumer)

		if span != nil {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "success")
			}
			span.SetAttributes(attribute.String("endTime", time.Now().Format("2006-01-02 15:04:05.999")))
			span.End()
		}
		return nil
	}, oFunc...)
}
