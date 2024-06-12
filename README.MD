# rocketmq-client-go

#### 介绍
go版rocketMQ客户端
对官方`github.com/apache/rocketmq-clients/golang/v5`对了封装，简化了使用方式，并增加了必填参数的友好提示。
此外，提供了`goFrame`框架的扩展包，支持生产者到消费者的链路追踪。

#### 依赖
- golang 1.21
- github.com/apache/rocketmq-clients/golang/v5

使用gf扩展的话：

- github.com/gogf/gf/v2 v2.7.1

#### 安装教程
```bash
go get -u github.com/yiqiang3344/rocketmq-client-go@latest
```

#### 使用说明
参考[example](example)

[direct](example%2Fdirect)中为直接使用的示例。

[gf](example%2Fgf)中为基于goFrame框架的使用示例。
