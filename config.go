package rocketmq_client

import (
	"errors"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"os"
	"strings"
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
	FlowColor        *string          //流量染色标识，为nil则表示不启用流量染色功能，生产者时表示流量染色标识，消费者时表示当前系统的染色标识
	FlowColorBase    *bool            //当前环境是否是基准环境，消费者使用，为nil则忽略，是基准系统时，可以匹配流量标识为空字符串的消息
}

func checkCfg(cfg *Config) error {
	if cfg.LogStdout {
		os.Setenv("mq.consoleAppender.enabled", "true")
	} else {
		os.Setenv("mq.consoleAppender.enabled", "false")
	}
	if strings.Trim(cfg.LogPath, "") == "" {
		cfg.LogPath = "/tmp"
	}
	os.Setenv("rocketmq.client.logRoot", cfg.LogPath)
	rmq_client.ResetLogger()

	if strings.Trim(cfg.Endpoint, "") == "" {
		return errors.New("Endpoint不能为空")
	}

	if strings.Trim(cfg.NameSpace, "") == "" {
		return errors.New("NameSpace不能为空")
	}
	return nil
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
