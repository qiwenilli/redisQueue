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
	q := NewQueue(cli, WithPrefix("qiwen"), WithName("q1"), WithAck(false))
	run_queue(q)
}

// func Test_queue_auto(t *testing.T) {
// 	q := NewQueue(cli, WithPrefix("qiwen"), WithName("q2"), WithAck(true))
// 	run_queue(q)
// }

func run_queue(q *queue) {

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
		consume(q, "1", count)
	}()
	go func() {
		consume(q, "2", count)
	}()
	consume(q, "3", count)
}

func consume(q *queue, prefix string, count *atomic.Int32) {
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

		q.Ack(context.Background())

		count.Inc()
	}
}
