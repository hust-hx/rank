package main

import (
	"context"
	"delayqueue"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go-redis/src/mq"
	"strings"

	// "time"
	"fmt"
	"go-redis/src/dao"
	"sync"
)

var redisAddr = "127.0.0.1:6379"
var redisPasswd = ""
var q *dao.DelayQueue

var db *dao.GovernDb

var c *mq.Consumer
var p *mq.Producer

var wg sync.WaitGroup

func handleMsg(ctx context.Context, msg ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	// TODO: consumer logic
	// get userId, redis key from msg
	for i := range msg {
		s := string(msg[i].Body)
		fmt.Printf("Get msg from MQ %s \n", s)
		ss := strings.Split(s, ",")
		userId, key := ss[0], ss[1]
		db.UpdateGovernStatus(userId, delayqueue.ExemptStatus)
		fmt.Printf("Update record %s \n", s)

		q.RemoveTaskFromDelayQueue(key, userId)

		wg.Done()
	}
	return consumer.ConsumeSuccess, nil
}

func handleEvent(key string) {
	for {
		userId, err := q.GetNextTaskFromDelayQueue(key)
		if err != nil || userId == "" {
			continue
		}
		fmt.Printf("Get userId = %s from Zset \n", userId)
		msg := userId + "," + key
		p.SendSyncMessage(msg, "testTopic1")
		fmt.Printf("Send msg to MQ \n")

		if err != nil {
			fmt.Printf("Remove from zset failed \n")
			continue
		}
	}
}

func event(keys []string) {
	for i := 0; i < len(keys); i++ {
		go handleEvent(keys[i])
	}
}

func lbAddTaskToDelayQueue(qKeys []string, userId string, punishTime int64) error {
	// hash by userId
	f := func(key string) int {
		val := 0
		for i := 0; i < len(key); i++ {
			val = val + int(key[i])
		}
		return val
	}
	idx := f(userId) % len(qKeys)
	err := q.AddTaskToDelayQueue(qKeys[idx], userId, punishTime)
	return err
}

func lbRemoveAllTasks(keys []string) {
	for i := 0; i < len(keys); i++ {
		q.RemoveAllTasks(keys[i])
	}
}

func main() {
	qKeys := []string{"DQ1", "DQ2"}
	q = dao.NewDelayQueue()
	if err := q.InitRedis(redisAddr, redisPasswd); err != nil {
		fmt.Printf("Init redis failed\n")
		panic(err)
	}
	lbRemoveAllTasks(qKeys)

	db = dao.NewGovernDb()
	db.LinkDb()

	p = mq.NewProducer()
	p.InitProducer()
	p.StartProducer()
	c = mq.NewConsumer()
	c.InitConsumer()

	go c.SubcribeTopic("testTopic1", handleMsg)

	userId := "954"
	punishTime := 10

	// add into Zset
	// q.AddTaskToDelayQueue(userId, int64(punishTime))
	lbAddTaskToDelayQueue(qKeys, userId, int64(punishTime))
	db.UserPunish(userId, dao.PunishStatus, dao.MultiAccountType, int64(punishTime))

	wg.Add(1)
	event(qKeys)

	fmt.Printf("Waiting for consumer! \n")

	wg.Wait()
}
