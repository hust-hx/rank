package mq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
)

type Producer struct {
	p rocketmq.Producer
}

func NewProducer() (p *Producer) {
	return &Producer{}
}

func (this *Producer) InitProducer() (err error) {
	endPoint := []string{"127.0.0.1:9876"}
	// 创建一个producer实例
	this.p, err = rocketmq.NewProducer(
		producer.WithNameServer(endPoint),
		producer.WithRetry(2),
		producer.WithGroupName("ProducerGroupName"),
	)

	return nil
}

func (this *Producer) StartProducer() {
	// 启动
	err := this.p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}
}

func (this *Producer) ShutdownProducer() {
	err := this.p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s", err.Error())
		os.Exit(1)
	}
}

func (this *Producer) CreateTopic(topicName string) {
	endPoint := []string{"127.0.0.1:9876"}
	// 创建主题
	testAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(endPoint)))
	if err != nil {
		fmt.Printf("connection error: %s\n", err.Error())
	}
	err = testAdmin.CreateTopic(context.Background(), admin.WithTopicCreate(topicName))
	if err != nil {
		fmt.Printf("createTopic error: %s\n", err.Error())
	}
}

func (this *Producer) SendSyncMessage(message string, topicName string) {
	// 发送消息
	result, err := this.p.SendSync(context.Background(), &primitive.Message{
		Topic: topicName,
		Body:  []byte(message),
	})

	if err != nil {
		fmt.Printf("send message error: %s\n", err.Error())
	} else {
		fmt.Printf("send message seccess: result=%s\n", result.String())
	}

}
