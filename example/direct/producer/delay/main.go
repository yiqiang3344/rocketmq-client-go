package main

import (
	"context"
	"encoding/json"
	"fmt"
	rocketmq_client "rocketmq_client/src"
	"time"
)

const (
	Endpoint     = "127.0.0.1:18081"
	NameSpace    = "test"
	AccessKey    = ""
	AccessSecret = ""
	// ./bin/mqadmin updateTopic -n 127.0.0.1:9876 -t test_delay_demo -c DefaultCluster -a +message.type=DELAY
	Topic = "test_delay_demo"
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
		resp, err := client.Send(
			ctx,
			rocketmq_client.TopicDelay,
			rocketmq_client.Message{
				Body:              fmt.Sprintf("msg%d", i),         //必填
				Topic:             Topic,                           //必填
				DeliveryTimestamp: time.Now().Add(1 * time.Minute), //必填
				Tag:               "test_delay",
				Keys:              []string{"test_delay"},
				Properties: map[string]string{
					"attr1": "1",
					"attr2": "2",
				},
			},
		)
		if err != nil {
			fmt.Printf("message [%d] produce failed:%v\n", i, err)
			continue
		}
		ret, _ := json.Marshal(resp)
		if err != nil {
			fmt.Printf("message [%d] json reps failed:%v\n", i, err)
		}
		fmt.Printf("message [%d] producer success:%s\n", i, ret)
	}
}
