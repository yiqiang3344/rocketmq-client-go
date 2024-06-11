package main

import (
	"context"
	"fmt"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"rocketmq_client"
	"time"
)

const (
	Endpoint     = "127.0.0.1:18081"
	NameSpace    = "test"
	AccessKey    = ""
	AccessSecret = ""
	Topic        = "test_normal_demo"
	// ./bin/mqadmin updateSubGroup -n 127.0.0.1:9876 -c cg_test_normal_demo -c DefaultCluster -o true
	ConsumerGroup = "cg_test_normal_demo"
)

func main() {
	var ctx = context.Background()
	client, err := rocketmq_client.GetClient(&rocketmq_client.Config{
		Endpoint:      Endpoint,      //必填
		NameSpace:     NameSpace,     //必填，要和生产者的保持一直
		ConsumerGroup: ConsumerGroup, //必填
		AccessKey:     AccessKey,
		AccessSecret:  AccessSecret,
		LogStdout:     true,
		Debug:         true,
	})
	if err != nil {
		panic(err)
	}
	stopFunc, err := client.SimpleConsume(
		ctx,
		func(ctx context.Context, msg *rmq_client.MessageView, consumer rocketmq_client.Consumer) {
			err := consumer.Ack(ctx)
			if err != nil {
				fmt.Printf("message [%s] ack failed:%v\n", msg.GetBody(), err)
				return
			}
			fmt.Printf("message [%s] ack success\n", msg.GetBody())
			return
		},
		rocketmq_client.WithConsumerOptionSubExpressions(map[string]*rmq_client.FilterExpression{
			Topic: rmq_client.SUB_ALL,
		}),
		rocketmq_client.WithConsumerOptionAwaitDuration(5*time.Second),
		rocketmq_client.WithConsumerOptionMaxMessageNum(5),
		rocketmq_client.WithConsumerOptionInvisibleDuration(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer stopFunc()
	time.Sleep(time.Minute)
}
