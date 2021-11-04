package redisQueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func NewQueue(cli redis.UniversalClient, channelName string, autoAck bool) *queue {
	return &queue{cli, channelName, channelName + "_lock", autoAck}
}

// 有序消费除列，必须ack, 消费未消费成功，不会返回数据
type queue struct {
	cli         redis.UniversalClient
	channelName string
	channelLock string
	autoAck     bool
}
type element struct {
	Val interface{}
}

func (e *element) MarshalBinary() (data []byte, err error) {
	return json.Marshal(e)
}

func (e *element) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, e)
}

func (q *queue) Add(ctx context.Context, val interface{}) error {
	return q.cli.LPush(ctx, q.channelName, &element{val}).Err()
}

func (q *queue) Recive(ctx context.Context) (interface{}, error) {

	var v interface{}
	err := q.cli.Watch(ctx, func(tx *redis.Tx) error {

		// command := `if redis.call("set", KEYS[1], ARGV[1], "ex", 10, "nx") then return "ok" else return "err"; end;`
		// eval := tx.Eval(ctx1, command, []string{"hit"}, true)

		if !q.autoAck {
			lock := tx.SetNX(ctx, q.channelLock, true, time.Hour)
			if lock.Err() != nil {
				return lock.Err()
			}
			if !lock.Val() {
				return fmt.Errorf("%s", "wait ack")
			}
		}

		cmd := tx.RPop(ctx, q.channelName)
		err := cmd.Err()
		if err != nil {
			if err == redis.Nil {
				for {
					err = tx.Del(ctx, q.channelLock).Err()
					if err != nil && err == redis.Nil {
						time.Sleep(time.Millisecond * 100)
						continue
					}
					break
				}
			}
			return err
		}
		elem := &element{}
		err = cmd.Scan(elem)
		if err != nil {
			return err
		}
		v = elem.Val
		return nil
	}, q.channelName)
	return v, err
}

func (q *queue) Ack(ctx context.Context) error {
	var err error
	for {
		err = q.cli.Del(ctx, q.channelLock).Err()
		if err != nil && err == redis.Nil {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}
	return err
}
