package event

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/the-anna-project/context"
	"github.com/the-anna-project/context/merge"
	"github.com/the-anna-project/id"
)

// SignalConfig represents the configuration used to create a new signal event.
type SignalConfig struct {
	// Settings.
	Arguments []reflect.Value
	Context   context.Context
	Created   time.Time
	ID        string
}

// DefaultSignalConfig provides a default configuration to create a new signal
// event by best effort.
func DefaultSignalConfig() SignalConfig {
	var newID string
	{
		idConfig := id.DefaultServiceConfig()
		idService, err := id.NewService(idConfig)
		if err != nil {
			panic(err)
		}
		newID, err = idService.New()
		if err != nil {
			panic(err)
		}
	}

	config := SignalConfig{
		// Settings.
		Arguments: nil,
		Context:   nil,
		Created:   time.Now(),
		ID:        newID,
	}

	return config
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
	if config.ID == "" {
		return nil, maskAnyf(invalidConfigError, "id must not be empty")
	}

	newSignal := &signal{
		// Internals.
		payload: "",

		// Settings.
		arguments: config.Arguments,
		context:   config.Context,
		created:   config.Created,
		id:        config.ID,
	}

	b, err := json.Marshal(newSignal)
	if err != nil {
		return nil, maskAny(err)
	}
	newSignal.payload = string(b)

	return newSignal, nil
}

func NewSignalFromEvent(event Event) (Signal, error) {
	var newSignal *signal
	err := json.Unmarshal([]byte(event.Payload()), newSignal)
	if err != nil {
		return nil, maskAny(err)
	}

	if newSignal.context == nil {
		return nil, maskAnyf(invalidConfigError, "context must not be empty")
	}
	if newSignal.created.IsZero() {
		return nil, maskAnyf(invalidConfigError, "created must not be empty")
	}
	if newSignal.id == "" {
		return nil, maskAnyf(invalidConfigError, "id must not be empty")
	}

	newSignal.payload = event.Payload()

	return newSignal, nil
}

func NewSignalFromSignals(signals []Signal) (Signal, error) {
	if len(signals) == 0 {
		return nil, maskAnyf(invalidConfigError, "signals must not be empty")
	}

	var err error

	var args []reflect.Value
	{
		for _, s := range signals {
			args = append(args, s.Arguments()...)
		}
	}

	var ctx context.Context
	{
		newCtx, err := context.New(context.DefaultConfig())
		if err != nil {
			return nil, maskAny(err)
		}

		var allCtxs []context.Context
		for _, s := range signals {
			allCtxs = append(allCtxs, s.Context())
		}

		ctx, err = merge.NewContextFromContexts(newCtx, allCtxs)
		if err != nil {
			return nil, maskAny(err)
		}
	}

	config := DefaultSignalConfig()
	config.Arguments = args
	config.Context = ctx
	newSignal, err := NewSignal(config)
	if err != nil {
		return nil, maskAny(err)
	}

	return newSignal, nil
}

type signal struct {
	// Internals.
	payload string `json:"-"`

	// Settings.
	arguments []reflect.Value `json:"arguments"`
	context   context.Context `json:"context"`
	created   time.Time       `json:"created"`
	id        string          `json:"id"`
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

func (s *signal) ID() string {
	return s.id
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

	s.payload = string(b)

	return nil
}

func (s *signal) Payload() string {
	return s.payload
}
