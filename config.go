package rocketmq_client

import (
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

type Config struct {
	Endpoint         string           //必填
	NameSpace        string           //必填
	ConsumerGroup    string           //使用消费者时，必填
	AccessKey        string           //可选
	AccessSecret     string           //可选
	LogPath          string           //官方rocketmq日志文件路径，默认为/tmp
	LogStdout        bool             //是否在终端输出官方rocketmq日志，输出的话则不会记录日志文件
	Debug            bool             //是否在终端输出本客户端的debug信息
	DebugHandlerFunc debugHandlerFunc //本客户端的debug信息处理方法，不管debug开没开，有debug信息的时候都会调用
}

func getRmqCfg(cfg *Config) *rmq_client.Config {
	return &rmq_client.Config{
		Endpoint:      cfg.Endpoint,
		NameSpace:     cfg.NameSpace,
		ConsumerGroup: cfg.ConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:     cfg.AccessKey,
			AccessSecret:  cfg.AccessSecret,
			SecurityToken: "",
		},
	}
}
