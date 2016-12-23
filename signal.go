package event

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/the-anna-project/context"
)

// SignalConfig represents the configuration used to create a new signal event.
type SignalConfig struct {
	// Settings.
	Arguments []reflect.Value
	Context   context.Context
	Created   time.Time
	Payload   string
}

// DefaultSignalConfig provides a default configuration to create a new signal
// event by best effort.
func DefaultSignalConfig() SignalConfig {
	return SignalConfig{
		// Settings.
		Arguments: nil,
		Context:   nil,
		Created:   time.Now(),
		Payload:   "",
	}
}

// NewSignal creates a new configured signal event.
func NewSignal(config SignalConfig) (Signal, error) {
	// Settings.
	if config.Context == nil {
		return nil, maskAnyf(invalidConfigError, "context must not be empty")
	}
	if config.Created.IsZero() {
		return nil, maskAnyf(invalidConfigError, "created must not be empty")
	}

	newEvent := &signal{
		// Settings.
		arguments: config.Arguments,
		context:   config.Context,
		created:   config.Created,
		payload:   config.Payload,
	}

	return newEvent, nil
}

type signal struct {
	// Settings.
	arguments []reflect.Value
	context   context.Context
	created   time.Time
	payload   string
}

func (s *signal) Arguments() []reflect.Value {
	return s.arguments
}

func (s *signal) Context() context.Context {
	return s.context
}

func (s *signal) Created() time.Time {
	return s.created
}

func (s *signal) MarshalJSON() ([]byte, error) {
	type Clone signal

	b, err := json.Marshal(&struct {
		*Clone
	}{
		Clone: (*Clone)(s),
	})
	if err != nil {
		return nil, maskAny(err)
	}

	return b, nil
}

func (s *signal) UnmarshalJSON(b []byte) error {
	type Clone signal

	aux := &struct {
		*Clone
	}{
		Clone: (*Clone)(s),
	}
	err := json.Unmarshal(b, &aux)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *signal) Payload() string {
	return s.payload
}
