package rocketmq_client

// TopicType 主题类型
type TopicType string

const (
	TopicNormal      TopicType = "Normal"
	TopicFIFO        TopicType = "FIFO"
	TopicDelay       TopicType = "Delay"
	TopicTransaction TopicType = "Transaction"
)
