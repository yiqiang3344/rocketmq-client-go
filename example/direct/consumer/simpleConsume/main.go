package main

import (
	"context"
	"errors"
	"fmt"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"math/rand"
	"rocketmq_client"
	"time"
)

const (
	Endpoint     = "127.0.0.1:18081"
	NameSpace    = "test"
	AccessKey    = ""
	AccessSecret = ""
	Topic1       = "test_normal_demo"
	Topic2       = "test_fifo_demo"
	Topic3       = "test_delay_demo"
	Topic4       = "test_transaction_demo"
	// ./bin/mqadmin updateSubGroup -n 127.0.0.1:9876 -c cg_test_demo -c DefaultCluster -o true
	ConsumerGroup = "cg_test_demo"
)

func main() {
	var ctx = context.Background()
	stopFunc, err := rocketmq_client.SimpleConsume(
		ctx,
		&rocketmq_client.Config{
			Endpoint:      Endpoint,      //必填
			NameSpace:     NameSpace,     //必填，要和生产者的保持一直
			ConsumerGroup: ConsumerGroup, //必填
			AccessKey:     AccessKey,
			AccessSecret:  AccessSecret,
			LogStdout:     false,
			Debug:         true,
		},
		func(ctx context.Context, msg *rmq_client.MessageView, consumer rocketmq_client.Consumer) error {
			//随机消费失败
			if rand.Intn(100) < 30 {
				fmt.Printf("message [%s] consumer failed\n", msg.GetBody())
				return errors.New(fmt.Sprintf("msg[%s]随机的消费失败测试", msg.GetBody()))
			}

			err := consumer.Ack(ctx)
			if err != nil {
				fmt.Printf("message [%s] ack failed:%v\n", msg.GetBody(), err)
				return err
			}
			fmt.Printf("message [%s] ack success\n", msg.GetBody())
			return nil
		},
		rocketmq_client.WithConsumerOptionSubExpressions(map[string]*rmq_client.FilterExpression{
			Topic1: rmq_client.SUB_ALL,
			Topic2: rmq_client.SUB_ALL,
			Topic3: rmq_client.SUB_ALL,
			Topic4: rmq_client.SUB_ALL,
		}),
		rocketmq_client.WithConsumerOptionAwaitDuration(5*time.Second),
		rocketmq_client.WithConsumerOptionMaxMessageNum(5),
		rocketmq_client.WithConsumerOptionInvisibleDuration(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer stopFunc()
	time.Sleep(5 * time.Minute)
}
