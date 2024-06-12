package main

import (
	"encoding/json"
	"fmt"
	"github.com/gogf/gf/contrib/trace/otlpgrpc/v2"
	"github.com/gogf/gf/v2/net/gtrace"
	"github.com/gogf/gf/v2/os/gctx"
	"go.opentelemetry.io/otel/codes"
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
	// ./bin/mqadmin updateTopic -n 127.0.0.1:9876 -t test_fifo_demo -c DefaultCluster -a +message.type=FIFO
	Topic        = "test_fifo_demo"
	MessageGroup = "messagegroup1"
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
	)
	if err != nil {
		panic(err)
	}
	defer producer.Stop()
	for i := 1; i <= 5; i++ {
		resp, err := producer.Send(ctx, rocketmq_client.TopicFIFO, rocketmq_client.Message{
			Body:         fmt.Sprintf("%smsg%d", rocketmq_client.TopicFIFO, i), //必填
			Topic:        Topic,                                                //必填
			MessageGroup: MessageGroup,                                         //必填
			Tag:          "test_fifo",
			Keys:         []string{"test_fifo"},
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
