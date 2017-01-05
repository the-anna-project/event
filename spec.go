package event

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/the-anna-project/context"
)

// Backoff represents the object managing backoff algorithms to retry actions.
type Backoff interface {
	// NextBackOff provides the duration expected to wait before retrying an
	// action. time.Duration = -1 indicates that no more retry should be
	// attempted.
	NextBackOff() time.Duration
	// Reset sets the backoff back to its initial state.
	Reset()
}

type Event interface {
	Created() time.Time
	ID() string
	json.Marshaler
	json.Unmarshaler
	Payload() string
}

type Service interface {
	// Boot initializes and starts the whole service like booting a machine. The
	// call to Boot blocks until the service is completely initialized, so you
	// might want to call it in a separate goroutine.
	Boot()
	// Create publishes the given event and associates it with the given labels.
	Create(event Event, labels ...string) error
	// Delete removes the given event which is associated with the given labels.
	//
	// Delete does not unqueue events. That is why delete must be called on an
	// event that was already consumed from a queue using Service.Search. In case
	// Delete is called on an event that is still queued, upcoming tries to
	// consume the deleted event will fail.
	Delete(event Event, labels ...string) error
	// ExistsAny checks whether there is any event queued associated within the
	// given labels.
	ExistsAny(labels ...string) (bool, error)
	// Limit trims the number of events within a labeled queue by cutting off
	// events from the queue's tail.
	Limit(max int, labels ...string) error
	// Search blocks until the next event associated with the given labels can be
	// returned. Consuming any event regardless their labeling can be done by
	// providing the wildcard label LabelWildcard.
	Search(labels ...string) (Event, error)
	// SearchAll returns all events associated with the given labels. While
	// Service.Search blocks until one event is available and can be returned,
	// Service.SearchAll returns all events at once and in case there is no single
	// event available, a not found error is returned.
	SearchAll(labels ...string) ([]Event, error)
	// Shutdown ends all processes of the service like shutting down a machine.
	// The call to Shutdown blocks until the service is completely shut down, so
	// you might want to call it in a separate goroutine.
	Shutdown()
	// WriteAll overwrites all events associated with the provided labels with the
	// given list of events, no matter if there have been events before or not.
	WriteAll(events []Event, labels ...string) error
}

type Signal interface {
	Arguments() []reflect.Value
	Context() context.Context
	Event
}
