package celery

import (
	"time"

	mapset "github.com/deckarep/golang-set/v2"
)

var (
	READY_STATES = mapset.NewSet("SUCCESS", "FAILURE", "REVOKED")
)

// ---------------------------------------------------------
//
//  Celery
//
// ---------------------------------------------------------

type CeleryBroker interface {
	SendCeleryMessage(string, *CeleryMessage) error
}

type CeleryBackend interface {
	GetCeleryResult(string) (*ResultMessage, error)
}

type Celery struct {
	Broker  CeleryBroker
	Backend CeleryBackend
}

func (c Celery) Delay(task string, args ...any) (*Result, error) {
	return c.DelayWithQueue("celery", task, args...)
}

func (c Celery) DelayWithQueue(queue string, task string, args ...any) (*Result, error) {
	cm := NewCeleryMessage(queue, task, args...)

	if err := c.Broker.SendCeleryMessage(queue, cm); err != nil {
		return nil, err
	}

	return &Result{
		ID:      cm.Headers.ID,
		Backend: c.Backend,
	}, nil
}

// ---------------------------------------------------------
//
//  CeleryResult
//
// ---------------------------------------------------------

type Result struct {
	ID      string
	Backend CeleryBackend
	Result  *ResultMessage
}

func (r *Result) Get() error {
	if r.Result == nil || !READY_STATES.Contains(r.Result.Status) {
		v, err := r.Backend.GetCeleryResult(r.ID)

		if err != nil {
			return err
		}

		r.Result = v
	}

	return nil
}

func (r *Result) Ready() bool {
	r.Get()

	if r.Result == nil || !READY_STATES.Contains(r.Result.Status) {
		return false
	}

	return true
}

func (r *Result) Success() bool {
	r.Get()

	if r.Result == nil || r.Result.Status != "SUCCESS" {
		return false
	}

	return true
}

func (r *Result) Wait(timeout time.Duration) bool {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)

	for {
		select {
		case <-timeoutChan:
			return false
		case <-ticker.C:
			if r.Ready() {
				return true
			}
		}
	}
}
