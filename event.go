// Package event implements event queue primitives to distribute events within
// the neural network.
package event

import (
	"encoding/json"
	"time"

	"github.com/the-anna-project/id"
)

// Config represents the configuration used to create a new event.
type Config struct {
	// Settings.
	Created time.Time
	ID      string
	Payload string
}

// DefaultConfig provides a default configuration to create a new event by best
// effort.
func DefaultConfig() Config {
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

	config := Config{
		// Settings.
		Created: time.Now(),
		ID:      newID,
		Payload: "",
	}

	return config
}

// New creates a new configured event.
func New(config Config) (Event, error) {
	// Settings.
	if config.Created.IsZero() {
		return nil, maskAnyf(invalidConfigError, "created must not be empty")
	}
	if config.ID == "" {
		return nil, maskAnyf(invalidConfigError, "id must not be empty")
	}

	newEvent := &event{
		// Settings.
		created: config.Created,
		id:      config.ID,
		payload: config.Payload,
	}

	b, err := json.Marshal(newEvent)
	if err != nil {
		return nil, maskAny(err)
	}
	newEvent.payload = string(b)

	return newEvent, nil
}

type event struct {
	// Settings.
	created time.Time `json:"created"`
	id      string    `json:"id"`
	payload string    `json:"payload"`
}

func (e *event) Created() time.Time {
	return e.created
}

func (e *event) ID() string {
	return e.id
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

	e.payload = string(b)

	return nil
}

func (e *event) Payload() string {
	return e.payload
}
