// Package event implements event queue primitives to distribute events within
// the neural network.
package event

import (
	"encoding/json"
	"time"
)

// Config represents the configuration used to create a new event.
type Config struct {
	// Settings.
	Created time.Time
	Payload string
}

// DefaultConfig provides a default configuration to create a new event by best
// effort.
func DefaultConfig() Config {
	return Config{
		// Settings.
		Created: time.Now(),
		Payload: "",
	}
}

// New creates a new configured event.
func New(config Config) (Event, error) {
	// Settings.
	if config.Created.IsZero() {
		return nil, maskAnyf(invalidConfigError, "created must not be empty")
	}

	newEvent := &event{
		// Settings.
		created: config.Created,
		payload: config.Payload,
	}

	return newEvent, nil
}

type event struct {
	// Settings.
	created time.Time
	payload string
}

func (e *event) Created() time.Time {
	return e.created
}

func (e *event) MarshalJSON() ([]byte, error) {
	type Clone event

	b, err := json.Marshal(&struct {
		*Clone
	}{
		Clone: (*Clone)(e),
	})
	if err != nil {
		return nil, maskAny(err)
	}

	return b, nil
}

func (e *event) UnmarshalJSON(b []byte) error {
	type Clone event

	aux := &struct {
		*Clone
	}{
		Clone: (*Clone)(e),
	}
	err := json.Unmarshal(b, &aux)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (e *event) Payload() string {
	return e.payload
}
