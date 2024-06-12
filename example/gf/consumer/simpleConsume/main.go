package main

import (
	"context"
	"errors"
	"fmt"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/gogf/gf/contrib/trace/otlpgrpc/v2"
	"github.com/gogf/gf/v2/os/gctx"
	"math/rand"
	"rocketmq_client"
	"time"
)

const (
	OtlpAppName    = "rocketmqClientTest"
	OtlpEndpoint   = "tracing-analysis-dc-bj.aliyuncs.com:8090"
	OtlpTraceToken = "xxxx_xxxx"
	Endpoint       = "127.0.0.1:18081"
	NameSpace      = "test"
	AccessKey      = ""
	AccessSecret   = ""
	Topic1         = "test_normal_demo"
	Topic2         = "test_fifo_demo"
	Topic3         = "test_delay_demo"
	Topic4         = "test_transaction_demo"
	// ./bin/mqadmin updateSubGroup -n 127.0.0.1:9876 -c cg_test_demo -c DefaultCluster -o true
	ConsumerGroup = "cg_test_demo"
)

func main() {
	var ctx = gctx.GetInitCtx()
	// 链路追踪初始化
	shutdown, err := otlpgrpc.Init(
		OtlpAppName,
		OtlpEndpoint,
		OtlpTraceToken,
	)
	if err != nil {
		panic(err)
	}
	defer shutdown()

	stopFunc, err := rocketmq_client.SimpleConsume4Gf(
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
			time.Sleep(100 * time.Millisecond)
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
	time.Sleep(10 * time.Minute)
}
