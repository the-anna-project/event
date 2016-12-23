package event

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/the-anna-project/context"
)

type Event interface {
	Created() time.Time
	json.Marshaler
	json.Unmarshaler
	Payload() string
}

type Service interface {
	Boot()
	Consume() (Event, error)
	Publish(event Event) error
	Shutdown()
}

type Signal interface {
	Arguments() []reflect.Value
	Context() context.Context
	Event
}
