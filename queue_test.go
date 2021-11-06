package redisQueue

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/atomic"
)

var (
	maxMsg int32 = 100

	cli redis.UniversalClient
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	opts := &redis.UniversalOptions{
		// Addrs:        []string{"192.168.177.132:6379"},
		Addrs:        []string{"127.0.0.1:6379"},
		DB:           0,
		Username:     "",
		Password:     "",
		MaxRetries:   3,
		DialTimeout:  time.Second * 3,
		PoolSize:     5,
		MinIdleConns: 1,
	}

	cli = redis.NewUniversalClient(opts)
	cli.FlushAll(context.Background())
}

func Test_queue(t *testing.T) {
	run_queue("q1", false)
}

// func Test_queue_auto(t *testing.T) {
// 	run_queue("q2", true)
// }

func run_queue(queueName string, autoAck bool) {

	q := NewQueue(cli, queueName, autoAck)
	go func() {
		var i int32 = 0
		for {
			if i >= maxMsg {
				break
			}

			err := q.Add(context.Background(), i)
			if err != nil {
				log.Println("errr:", err)
			}
			// log.Println("add ", i)
			i++
		}
	}()

	count := atomic.NewInt32(0)
	go func() {
		consume(q, "1", count, autoAck)
	}()
	go func() {
		consume(q, "2", count, autoAck)
	}()
	consume(q, "3", count, autoAck)
}

func consume(q *queue, prefix string, count *atomic.Int32, autoAck bool) {
	for {
		if count.Load() >= maxMsg {
			break
		}

		v, err := q.Recive(context.Background())
		_ = v
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println(prefix, "_consume----> ", v, err, count.Load())

		if !autoAck {
			q.Ack(context.Background())
		}

		count.Inc()
	}
}
