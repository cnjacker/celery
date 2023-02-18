package celery

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	REDIS_ID_PREFIX   = "celery-task-meta-"
	REDIS_TIME_LAYOUT = "2006-01-02T15:04:05.999999"
)

// ---------------------------------------------------------
//
//  RedisBroker
//
// ---------------------------------------------------------

type RedisBroker struct {
	Conn *redis.Client
	Ctx  context.Context
}

func NewRedisBroker(url string) *RedisBroker {
	if v, err := redis.ParseURL(url); err == nil {
		return &RedisBroker{
			Conn: redis.NewClient(v),
			Ctx:  context.Background(),
		}
	}

	return nil
}

func (r RedisBroker) SendCeleryMessage(queue string, cm *CeleryMessage) error {
	v, err := json.Marshal(cm)

	if err != nil {
		return err
	}

	return r.Conn.LPush(r.Ctx, queue, string(v)).Err()
}

// ---------------------------------------------------------
//
//  RedisBackend
//
// ---------------------------------------------------------

type RedisBackend struct {
	Conn *redis.Client
	Ctx  context.Context
}

func NewRedisBackend(url string) *RedisBackend {
	if v, err := redis.ParseURL(url); err == nil {
		return &RedisBackend{
			Conn: redis.NewClient(v),
			Ctx:  context.Background(),
		}
	}

	return nil
}

func (r RedisBackend) GetCeleryResult(ID string) (*ResultMessage, error) {
	bytes, err := r.Conn.Get(r.Ctx, REDIS_ID_PREFIX+ID).Bytes()

	if err != nil {
		return nil, err
	}

	data := map[string]any{}

	if err := json.Unmarshal(bytes, &data); err != nil {
		return nil, err
	}

	message := ResultMessage{
		ID:        data["task_id"].(string),
		Status:    data["status"].(string),
		Result:    data["result"],
		Traceback: data["traceback"],
		Children:  data["children"].([]any),
	}

	if v, err := time.Parse(REDIS_TIME_LAYOUT, data["date_done"].(string)); err == nil {
		message.DateDone = &v
	}

	return &message, nil
}
