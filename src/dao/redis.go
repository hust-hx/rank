package dao

import (
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

type DelayQueue struct {
	redisClient *redis.Client
}

func NewDelayQueue() *DelayQueue {
	return &DelayQueue{}
}

// init redis connect
func (q *DelayQueue) InitRedis(redisAddr string, redisPasswd string) error {
	q.redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPasswd,
		DB:       0,
	})
	_, err := q.redisClient.Ping().Result()
	if err != nil {
		return err
	}
	return nil
}

// add task into delay queue
func (q *DelayQueue) AddTaskToDelayQueue(key string, userId string, delayTime int64) error {
	_, err := q.redisClient.ZAdd(key, redis.Z{
		Score:  float64(time.Now().Unix() + delayTime),
		Member: userId,
	}).Result()
	if err != nil {
		return err
	}
	fmt.Printf("Add task into DQ with key " + key + ", userId " + userId + "\n")
	return nil
}

// Get next taskId from Queue
func (q *DelayQueue) GetNextTaskFromDelayQueue(key string) (string, error) {
	now := time.Now().Unix()
	items, err := q.redisClient.ZRangeByScore(key, redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now, 10),
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		return "", err
	}
	if len(items) == 0 {
		return "", nil
	}
	fmt.Printf("Get next task from DQ with key " + key + ", userId " + items[0] + "\n")
	return items[0], nil
}

// Remove Task
func (q *DelayQueue) RemoveTaskFromDelayQueue(key string, userId string) error {
	_, err := q.redisClient.ZRem(key, userId).Result()
	if err != nil {
		return err
	}
	return nil
}

func (q *DelayQueue) RemoveAllTasks(key string) {
	now := time.Now().Unix()
	q.redisClient.ZRemRangeByScore(key, "-inf", strconv.FormatInt(now, 10))
	fmt.Printf("Remove Zset with key " + key + "\n")
}
