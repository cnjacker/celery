package celery

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/oklog/ulid/v2"
)

// ---------------------------------------------------------
//
//  CeleryBody
//
// ---------------------------------------------------------

type CeleryBodyData struct {
	Callbacks []string `json:"callbacks"`
	Errbacks  []string `json:"errbacks"`
	Chain     []string `json:"chain"`
	Chord     *string  `json:"chord"`
}

type CeleryBody struct {
	Args   []any          `json:"args"`
	Kwargs map[string]any `json:"kwargs"`
	Data   CeleryBodyData `json:"data"`
}

func (cb CeleryBody) Encode() string {
	if len(cb.Args) == 0 {
		cb.Args = []any{}
	}

	if len(cb.Kwargs) == 0 {
		cb.Kwargs = map[string]any{}
	}

	var data = []any{cb.Args, cb.Kwargs, cb.Data}

	if v, err := json.Marshal(data); err == nil {
		return base64.StdEncoding.EncodeToString(v)
	}

	return base64.StdEncoding.EncodeToString([]byte(""))
}

func (cb CeleryBody) ArgsRepr() string {
	if len(cb.Args) == 0 {
		cb.Args = []any{}
	}

	if v, err := json.Marshal(cb.Args); err == nil {
		return string(v)
	}

	return "[]"
}

// ---------------------------------------------------------
//
//  CeleryMessage
//
// ---------------------------------------------------------

type CeleryHeaders struct {
	Lang         string    `json:"lang"`
	Task         string    `json:"task"`
	ID           string    `json:"id"`
	Shadow       *string   `json:"shadow"`
	Eta          *string   `json:"eta"`
	Expires      *string   `json:"expires"`
	Group        *int      `json:"group"`
	GroupIndex   *int      `json:"group_index"`
	Retries      int       `json:"retries"`
	TimeLimit    []*string `json:"timelimit"`
	RootID       string    `json:"root_id"`
	ParentID     *int      `json:"parent_id"`
	ArgsRepr     string    `json:"argsrepr"`
	KwargsRepr   string    `json:"kwargsrepr"`
	Origin       string    `json:"origin"`
	IgnoreResult bool      `json:"ignore_result"`
}

type CeleryDeliveryInfo struct {
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
}

type CeleryProperties struct {
	CorrelationID string             `json:"correlation_id"`
	ReplyTo       string             `json:"reply_to"`
	DeliveryMode  int                `json:"delivery_mode"`
	DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
	Priority      int                `json:"priority"`
	BodyEncoding  string             `json:"body_encoding"`
	DeliveryTag   string             `json:"delivery_tag"`
}

type CeleryMessage struct {
	Body            string           `json:"body"`
	ContentEncoding string           `json:"content-encoding"`
	ContentType     string           `json:"content-type"`
	Headers         CeleryHeaders    `json:"headers"`
	Properties      CeleryProperties `json:"properties"`
}

func NewCeleryMessage(queue string, task string, args ...any) *CeleryMessage {
	var id = ulid.Make().String()
	var hostname = "unknown"

	if v, err := os.Hostname(); err == nil {
		hostname = v
	}

	cb := CeleryBody{
		Args: args,
	}

	return &CeleryMessage{
		Body:            cb.Encode(),
		ContentEncoding: "utf-8",
		ContentType:     "application/json",
		Headers: CeleryHeaders{
			Lang:         "go",
			Task:         task,
			ID:           id,
			Shadow:       nil,
			Eta:          nil,
			Expires:      nil,
			Group:        nil,
			GroupIndex:   nil,
			Retries:      0,
			TimeLimit:    []*string{nil, nil},
			RootID:       id,
			ParentID:     nil,
			ArgsRepr:     cb.ArgsRepr(),
			KwargsRepr:   "{}",
			Origin:       fmt.Sprintf("%d@%s", os.Getpid(), hostname),
			IgnoreResult: false,
		},
		Properties: CeleryProperties{
			CorrelationID: id,
			ReplyTo:       ulid.Make().String(),
			DeliveryMode:  2,
			DeliveryInfo: CeleryDeliveryInfo{
				Exchange:   queue,
				RoutingKey: queue,
			},
			Priority:     0,
			BodyEncoding: "base64",
			DeliveryTag:  ulid.Make().String(),
		},
	}
}

// ---------------------------------------------------------
//
//  ResultMessage
//
// ---------------------------------------------------------

type ResultMessage struct {
	ID        string     `json:"task_id"`
	Status    string     `json:"status"`
	Result    any        `json:"result"`
	Traceback any        `json:"traceback"`
	Children  []any      `json:"children"`
	DateDone  *time.Time `json:"date_done"`
}
