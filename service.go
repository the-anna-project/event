package event

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/the-anna-project/storage"
)

const (
	// KindSignal represents the event service responsible for managing signal
	// events.
	KindSignal = "signal"
)

// ServiceConfig represents the configuration used to create a new event
// service.
type ServiceConfig struct {
	// Dependencies.
	StorageCollection *storage.Collection

	// Settings.
	Kind string
}

// DefaultServiceConfig provides a default configuration to create a new event
// service by best effort.
func DefaultServiceConfig() ServiceConfig {
	var err error

	var storageCollection *storage.Collection
	{
		storageConfig := storage.DefaultCollectionConfig()
		storageCollection, err = storage.NewCollection(storageConfig)
		if err != nil {
			panic(err)
		}
	}

	config := ServiceConfig{
		// Dependencies.
		StorageCollection: storageCollection,

		// Settings.
		Kind: "",
	}

	return config
}

// NewService creates a new configured event service.
func NewService(config ServiceConfig) (Service, error) {
	// Dependencies.
	if config.StorageCollection == nil {
		return nil, maskAnyf(invalidConfigError, "storage collection must not be empty")
	}

	// Settings.
	if config.Kind == "" {
		return nil, maskAnyf(invalidConfigError, "kind must not be empty")
	}
	if config.Kind != KindSignal {
		return nil, maskAnyf(invalidConfigError, "kind must be %s", KindSignal)
	}

	newService := &service{
		// Dependencies.
		storage: config.StorageCollection,

		// Internals.
		bootOnce:     sync.Once{},
		closer:       make(chan struct{}, 1),
		shutdownOnce: sync.Once{},

		// Settings.
		kind: config.Kind,
	}

	return newService, nil
}

type service struct {
	// Dependencies.
	storage *storage.Collection

	// Internals.
	bootOnce     sync.Once
	closer       chan struct{}
	shutdownOnce sync.Once

	// Settings.
	kind string
}

func (s *service) Boot() {
	s.bootOnce.Do(func() {
		// Service specific boot logic goes here.
	})
}

func (s *service) Consume() (Event, error) {
	result, err := s.storage.Queue.PopFromList(s.key())
	if err != nil {
		return nil, maskAny(err)
	}

	event, err := New(DefaultConfig())
	if err != nil {
		return nil, maskAny(err)
	}
	err = json.Unmarshal([]byte(result), &event)
	if err != nil {
		return nil, maskAny(err)
	}

	return event, nil
}

func (s *service) Publish(event Event) error {
	b, err := json.Marshal(event)
	if err != nil {
		return maskAny(err)
	}

	err = s.storage.Queue.PushToList(s.key(), string(b))
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}

func (s *service) key() string {
	return fmt.Sprintf("event:%s", s.kind)
}
