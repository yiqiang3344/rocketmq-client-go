package main

import (
	"context"
	"encoding/json"
	"fmt"
	"rocketmq_client"
)

const (
	Endpoint     = "127.0.0.1:18081"
	NameSpace    = "test"
	AccessKey    = ""
	AccessSecret = ""
	// ./bin/mqadmin updateTopic -n 127.0.0.1:9876 -t test_normal_demo -c DefaultCluster -a +message.type=NORMAL
	Topic = "test_normal_demo"
)

func main() {
	var ctx = context.Background()
	producer, err := rocketmq_client.GetProducer(
		&rocketmq_client.Config{
			Endpoint:     Endpoint,
			NameSpace:    NameSpace,
			AccessKey:    AccessKey,
			AccessSecret: AccessSecret,
			LogStdout:    false,
			Debug:        true,
		},
		rocketmq_client.WithProducerOptionTopics(Topic),
		rocketmq_client.WithProducerOptionMaxAttempts(3),
	)
	if err != nil {
		panic(err)
	}
	defer producer.Stop()
	for i := 1; i <= 5; i++ {
		resp, err := producer.Send(ctx, rocketmq_client.TopicNormal, rocketmq_client.Message{
			Body:  fmt.Sprintf("%smsg%d", rocketmq_client.TopicNormal, i), //必填
			Topic: Topic,                                                  //必填
			Tag:   "test_normal",
			Keys:  []string{"test_normal"},
			Properties: map[string]string{
				"attr1": "1",
				"attr2": "2",
			},
		})
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
