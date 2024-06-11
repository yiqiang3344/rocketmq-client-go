package main

import (
	"context"
	"encoding/json"
	"fmt"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	rocketmq_client "rocketmq_client/src"
	"time"
)

const (
	Endpoint     = "127.0.0.1:18081"
	NameSpace    = "test"
	AccessKey    = ""
	AccessSecret = ""
	// ./bin/mqadmin updateTopic -n 127.0.0.1:9876 -t test_normal_demo -c DefaultCluster -a +message.type=Normal
	Topic = "test_normal_demo"
)

func main() {
	var ctx = context.Background()
	client, err := rocketmq_client.GetClient(&rocketmq_client.Config{
		Endpoint:     Endpoint,
		NameSpace:    NameSpace,
		AccessKey:    AccessKey,
		AccessSecret: AccessSecret,
		LogStdout:    true,
		Debug:        true,
	})
	if err != nil {
		panic(err)
	}
	err = client.StartProducer(
		ctx,
		rocketmq_client.WithProducerOptionTopics([]string{Topic}),
		rocketmq_client.WithProducerOptionMaxAttempts(3),
	)
	if err != nil {
		panic(err)
	}
	defer client.StopProducer()
	for i := 1; i <= 5; i++ {
		err = client.SendAsync(
			ctx,
			rocketmq_client.TopicNormal,
			rocketmq_client.Message{
				Body:  fmt.Sprintf("msg%d", i), //必填
				Topic: Topic,                   //必填
				Tag:   "test_normal_async",
				Keys:  []string{"test_normal_async"},
				Properties: map[string]string{
					"attr1": "1",
					"attr2": "2",
				},
			},
			func(ctx context.Context, msg rocketmq_client.Message, resp []*rmq_client.SendReceipt, err error) {
				ret, _ := json.Marshal(resp)
				if err != nil {
					fmt.Printf("message [%s] json reps failed:%v\n", msg.Body, err)
				}
				fmt.Printf("message [%s] producer success:%s\n", msg.Body, ret)
				return
			}, //必填
		)
		if err != nil {
			fmt.Printf("message [%d] produce failed:%v\n", i, err)
			continue
		}
	}
	time.Sleep(10 * time.Second)
}