package event

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/the-anna-project/context"
)

type Event interface {
	Created() time.Time
	ID() string
	json.Marshaler
	json.Unmarshaler
	Payload() string
}

type Service interface {
	Boot()
	Consume() (Event, error)
	Delete(event Event) error
	// ExistsAnyWithLabel checks whether there is any event associated with the
	// given label.
	ExistsAnyWithLabel(label string) (bool, error)
	Publish(event Event) error
	PublishWithLabels(event Event, labels ...string) error
	Shutdown()
}

type Signal interface {
	Arguments() []reflect.Value
	Context() context.Context
	Event
}
