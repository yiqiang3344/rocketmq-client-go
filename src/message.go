package rocketmq_client

import (
	"context"
	"errors"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"strings"
	"time"
)

type Message struct {
	Body              string            //消息内容，必填
	Topic             string            //主题，必填
	Tag               string            //标签，可选
	MessageGroup      string            //消息组，FIFO消息类型必填，其他可选
	Keys              []string          //索引列表，可选
	Properties        map[string]string //自定义属性，可选
	DeliveryTimestamp time.Time         //延迟时间，Delay消息类型必填，其他可选
}

// initMsg 包装消息
func initMsg(ctx context.Context, cfg *Config, topicType TopicType, message Message) (msg *rmq_client.Message, err error) {
	//校验
	if strings.Trim(message.Topic, "") == "" {
		err = errors.New("topic必填")
		debugLog(cfg, "消息初始化失败:%v", err)
		return
	}
	if strings.Trim(message.Body, "") == "" {
		err = errors.New("body必填")
		debugLog(cfg, "消息初始化失败:%v", err)
		return
	}
	switch topicType {
	case TopicFIFO:
		if strings.Trim(message.MessageGroup, "") == "" {
			err = errors.New("FIFO消息类型messageGroup必填")
			debugLog(cfg, "消息初始化失败:%v", err)
			return
		}
	case TopicDelay:
		if message.DeliveryTimestamp.IsZero() {
			err = errors.New("Delay消息类型deliveryTimestamp必填")
			debugLog(cfg, "消息初始化失败:%v", err)
			return
		}
	}

	//初始化消息体
	msg = &rmq_client.Message{
		Topic: message.Topic,
		Body:  []byte(message.Body),
	}
	//设置消息tag
	if message.Tag != "" {
		msg.SetTag(message.Tag)
	}
	//设置消息组（只有FIFO类型topic可用）
	if topicType == TopicFIFO && message.MessageGroup != "" {
		msg.SetMessageGroup(message.MessageGroup)
	}
	//设置消息key
	msg.SetKeys(message.Keys...)
	//设置消息属性
	if len(message.Properties) > 0 {
		for k, v := range message.Properties {
			msg.AddProperty(k, v)
		}
	}
	//设置延迟时间（只有Delay类型topic可用）
	if topicType == TopicDelay && !message.DeliveryTimestamp.IsZero() {
		msg.SetDelayTimestamp(message.DeliveryTimestamp)
	}
	return
}

// IsTooManyRequest 是否触发了流控
func IsTooManyRequest(err error) bool {
	//如果是重试失败，则判断是否设置了补偿机制，有则调用
	if e, ok := err.(*rmq_client.ErrRpcStatus); ok && e.GetCode() == int32(v2.Code_TOO_MANY_REQUESTS) {
		return true
	}
	return false
}

// IsNoNewMessage 是否没有新消息
func IsNoNewMessage(err error) bool {
	//如果是重试失败，则判断是否设置了补偿机制，有则调用
	if e, ok := err.(*rmq_client.ErrRpcStatus); ok && e.GetCode() == int32(v2.Code_MESSAGE_NOT_FOUND) {
		return true
	}
	return false
}

// Send 同步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func Send(ctx context.Context, cfg *Config, producer rmq_client.Producer, topicType TopicType, msg Message) (resp []*rmq_client.SendReceipt, err error) {
	if topicType == TopicTransaction {
		err = errors.New("此方法不支持发送Transaction消息")
		debugLog(cfg, "消息发送失败:%v", err)
		return
	}
	message, err := initMsg(ctx, cfg, topicType, msg)
	if err != nil {
		debugLog(cfg, "消息发送失败:%v", err)
		return
	}

	resp, err = producer.Send(ctx, message)
	if err != nil {
		debugLog(cfg, "消息发送失败:%v", err)
		return
	}
	return
}

type SendAsyncDealFunc func(ctx context.Context, msg Message, resp []*rmq_client.SendReceipt, err error)

// SendAsync 异步发送消息
// 可支持普通、延迟、顺序类型的消息，不支持事务消息
func SendAsync(ctx context.Context, cfg *Config, producer rmq_client.Producer, topicType TopicType, msg Message, dealFunc SendAsyncDealFunc) (err error) {
	if dealFunc == nil {
		err = errors.New("dealFunc必填")
		debugLog(cfg, "消息发送失败:%v", err)
		return
	}

	if topicType == TopicTransaction {
		err = errors.New("此方法不支持发送Transaction消息")
		debugLog(cfg, "消息发送失败:%v", err)
		return
	}

	message, err := initMsg(ctx, cfg, topicType, msg)
	if err != nil {
		return err
	}

	producer.SendAsync(ctx, message, func(ctx context.Context, receipts []*rmq_client.SendReceipt, err error) {
		dealFunc(ctx, msg, receipts, err)
	})
	return
}

// ConfirmFunc 二次确认方法
// 注意：不要异步处理，本地事务逻辑提交时返回true，否则返回false
type ConfirmFunc func(msg Message, resp []*rmq_client.SendReceipt) bool

// SendTransaction 发送事务消息
// 注意：事务消息的生产者不能和其他类型消息的生产者共用
func SendTransaction(ctx context.Context, cfg *Config, producer rmq_client.Producer, message Message, confirmFunc ConfirmFunc) (resp []*rmq_client.SendReceipt, err error) {
	if confirmFunc == nil {
		err = errors.New("confirmFunc必填")
		debugLog(cfg, "消息发送失败:%v", err)
		return
	}

	msg, err := initMsg(ctx, cfg, TopicTransaction, message)
	if err != nil {
		return
	}

	transaction := producer.BeginTransaction()
	resp, err = producer.SendWithTransaction(ctx, msg, transaction)
	if err != nil {
		debugLog(cfg, "消息发送失败:%v", err)
		return
	}
	if confirmFunc(message, resp) {
		return resp, transaction.Commit()
	}
	return nil, transaction.RollBack()
}
