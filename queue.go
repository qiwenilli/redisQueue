package redisQueue

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/go-redis/redis/v8"
)

var (
	ERR_WAIT_ACK            = errors.New("wait ack")
	ERR_ASSERT_STRING_FAILD = errors.New("assert string faild")
)

// 有序消费除列，必须ack, 消费未消费成功，不会返回数据
type Queue interface {
	Add(ctx context.Context, val interface{}) error
	Recive(ctx context.Context) (interface{}, error)
	Ack(ctx context.Context) error
}

type queue struct {
	cli       redis.UniversalClient
	queueName string
	queueLock string
	autoAck   bool
}
type element struct {
	Val interface{}
}

func NewQueue(cli redis.UniversalClient, opts ...option) Queue {

	queueOpt := &queueOption{}
	for _, opt := range opts {
		opt(queueOpt)
	}
	return &queue{
		cli,
		queueOpt.prefix + "-" + queueOpt.name,
		queueOpt.prefix + "-" + queueOpt.name + "_lock",
		queueOpt.autoAck,
	}
}

func (e *element) MarshalBinary() (data []byte, err error) {
	return json.Marshal(e)
}

func (e *element) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, e)
}

func (q *queue) Add(ctx context.Context, val interface{}) error {
	return q.cli.LPush(ctx, q.queueName, &element{val}).Err()
}

func (q *queue) Recive(ctx context.Context) (interface{}, error) {

	var v interface{}
	err := func() error {

		var err error
		var cmd *redis.StringCmd
		if !q.autoAck {
			script := `local len = redis.call("LLen",KEYS[2])
if len > 0 then
	return nil
else
	return redis.call("RPopLPush",KEYS[1],KEYS[2])
end`
			cmd := q.cli.Eval(ctx, script, []string{q.queueName, q.queueLock})
			if cmd.Err() != nil {
				return cmd.Err()
			}
			if cmd.Val() == nil {
				return ERR_WAIT_ACK
			}
			err = nil
			//
			jsonData, ok := cmd.Val().(string)
			if ok {
				elem := &element{}
				err = json.Unmarshal([]byte(jsonData), elem)
				if err == nil {
					v = elem.Val
				}
			} else {
				err = ERR_ASSERT_STRING_FAILD
			}

		} else {
			cmd = q.cli.RPop(ctx, q.queueName)
			if err == redis.Nil {
				return nil
			}
			if err != nil {
				return err
			}

			elem := &element{}
			err = cmd.Scan(elem)
			if err != nil {
				return err
			}
			v = elem.Val
		}

		return err
	}()
	return v, err
}

func (q *queue) Ack(ctx context.Context) error {
	if q.autoAck {
		return nil
	}

	script := `local len = redis.call("LLen",KEYS[1])
if len > 0 then
	return redis.call("RPop",KEYS[1])
else
	return nil
end`
	cmd := q.cli.Eval(ctx, script, []string{q.queueLock})
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}
