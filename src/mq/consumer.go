package mq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"os"
)

type Consumer struct {
	c rocketmq.PushConsumer
}

func NewConsumer() (c *Consumer) {
	return &Consumer{}
}

func (this *Consumer) InitConsumer() (err error) {
	// 订阅主题、消费
	endPoint := []string{"127.0.0.1:9876"}
	// 创建一个consumer实例
	this.c, err = rocketmq.NewPushConsumer(consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("ConsumerGroupName"),
	)
	return err
}

func (this *Consumer) SubcribeTopic(topicName string, f func(ctx context.Context, msgs ...*primitive.MessageExt) (
	consumer.ConsumeResult, error)) {
	// 订阅topic
	err := this.c.Subscribe(topicName, consumer.MessageSelector{}, f)
	this.StartConsumer()
	if err != nil {
		fmt.Printf("subscribe message error: %s\n", err.Error())
	}
}

func (this *Consumer) StartConsumer() {
	// 启动consumer
	err := this.c.Start()

	if err != nil {
		fmt.Printf("consumer start error: %s\n", err.Error())
		os.Exit(-1)
	}
}

func (this *Consumer) ShutdownConsumer() {
	err := this.c.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s\n", err.Error())
	}
}
