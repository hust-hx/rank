package delayqueue

import (
	"strconv"
	"time"
	"fmt"
	"os"
	"sync"
	"math/rand"
	"encoding/hex"
	"github.com/go-redis/redis"
)

type DelayQueue struct {
	
}

func NewDelayQueue() *DelayQueue {
	return &DelayQueue{}
}

// init redis connect
func (q *DelayQueue)InitRedis(redisAddr string, redisPasswd string) error {
	redisClient = redis.NewClient(&redis.Options{
		Addr:		redisAddr,
		Password:	redisPasswd,
		DB:		0,
	})
	_, err := redisClient.Ping().Result()
	if err != nil {
		return err
	}
	return nil
}

// add task into delay queue
func (q *DelayQueue)AddTaskToDelayQueue(taskId string, delayTime int64) error {
	_, err := redisClient.ZAdd("DelayQueue", redis.Z{
		Score:		float64(time.Now().Unix() + delayTime)	,
		Member:		taskId,
	}).Result()
	if err != nil {
		return err
	}

	return nil
}

// Get next taskId from Queue
func (q *DelayQueue)GetNextTaskFromDelayQueue() (string, error) {
	now := time.Now().Unix()
	items, err := redisClient.ZRangeByScore("DelayQueue", redis.ZRangeBy{
		Min:		"-inf",
		Max:		strconv.FormatInt(now, 10),
		Offset:		0,
		Count:		1,
	}).Result()
	if err != nil {
		return "", err
	}
	if len(items) == 0 {
		return "", nil
	}
	return items[0], nil
}

// Remove Task 
func (q *DelayQueue)RemoveTaskFromDelayQueue(taskId string) error {
	_, err := redisClient.ZRem("DelayQueue", taskId).Result()
	if err != nil {
		return err
	}
	return nil
}

func (q *DelayQueue)consume(fn func(userId string) error) {
	for {	
		taskId, err := GetNextTaskFromDelayQueue()
		if err != nil || taskId == "" {
			continue
		}
		
		err = fn(taskId)
		if err != nil {
			continue
		}
		err = RemoveTaskFromDelayQueue(taskId)
		if err != nil {
			fmt.Printf("Remove from zset failed\n")
			continue
		}
	}	
}

