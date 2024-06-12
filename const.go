package rocketmq_client

// TopicType 主题类型
type TopicType string

const (
	TopicNormal      TopicType = "NORMAL"
	TopicFIFO        TopicType = "FIFO"
	TopicDelay       TopicType = "DELAY"
	TopicTransaction TopicType = "TRANSACTION"
)
