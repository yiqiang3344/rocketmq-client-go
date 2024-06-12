package main

import (
	"encoding/json"
	"fmt"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/gogf/gf/contrib/trace/otlpgrpc/v2"
	"github.com/gogf/gf/v2/net/gtrace"
	"github.com/gogf/gf/v2/os/gctx"
	"go.opentelemetry.io/otel/codes"
	"math/rand"
	"rocketmq_client"
)

const (
	OtlpAppName    = "rocketmqClientTest"
	OtlpEndpoint   = "tracing-analysis-dc-bj.aliyuncs.com:8090"
	OtlpTraceToken = "xxxx_xxxx"
	Endpoint       = "127.0.0.1:18081"
	NameSpace      = "test"
	AccessKey      = ""
	AccessSecret   = ""
	// ./bin/mqadmin updateTopic -n 127.0.0.1:9876 -t test_transaction_demo -c DefaultCluster -a +message.type=TRANSACTION
	Topic = "test_transaction_demo"
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

	fmt.Printf("tranceId:%s\n", gctx.CtxId(ctx))

	ctx, span := gtrace.NewSpan(ctx, "gfRocketmqProducerExample")
	defer func() {
		if e := recover(); e != nil {
			span.SetStatus(codes.Error, e.(error).Error())
			span.End()
			panic(e)
		} else {
			span.SetStatus(codes.Ok, "")
			span.End()
		}
		fmt.Println("span[gfRocketmqProducerExample] end")
	}()

	producer, err := rocketmq_client.GetGfProducer(
		&rocketmq_client.Config{
			Endpoint:     Endpoint,
			NameSpace:    NameSpace,
			AccessKey:    AccessKey,
			AccessSecret: AccessSecret,
			LogStdout:    true,
			Debug:        true,
		},
		rocketmq_client.WithProducerOptionTopics(Topic),
		rocketmq_client.WithProducerOptionMaxAttempts(3),
		rocketmq_client.WithProducerOptionTransactionChecker(func(msg *rmq_client.MessageView) rmq_client.TransactionResolution {
			fmt.Printf("message [%s] checker done\n", msg.GetBody())
			return rmq_client.COMMIT
		}),
	)
	if err != nil {
		panic(err)
	}
	defer producer.Stop()
	for i := 1; i <= 5; i++ {
		err := producer.SendTransaction(
			ctx,
			rocketmq_client.Message{
				Body:  fmt.Sprintf("%smsg%d", rocketmq_client.TopicTransaction, i), //必填
				Topic: Topic,                                                       //必填
				Tag:   "test_transaction",
				Keys:  []string{"test_transaction"},
				Properties: map[string]string{
					"attr1": "1",
					"attr2": "2",
				},
			},
			func(msg rocketmq_client.Message, resp []*rmq_client.SendReceipt) bool {
				ret, _ := json.Marshal(resp)
				if err != nil {
					fmt.Printf("message [%s] json reps failed:%v\n", msg.Body, err)
				}
				fmt.Printf("message [%s] producer success:%s\n", msg.Body, ret)
				res := true
				if rand.Intn(100) > 50 {
					res = false
				}
				fmt.Printf("message [%s] confirm %t\n", msg.Body, res)
				return res
			},
		)
		if err != nil {
			fmt.Printf("message [%d] produce failed:%v\n", i, err)
			continue
		}
	}
}
