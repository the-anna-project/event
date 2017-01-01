package event

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/the-anna-project/index"
	"github.com/the-anna-project/storage"
)

const (
	// KindSignal represents the event service responsible for managing signal
	// events.
	KindSignal      = "signal"
	NamespaceEvent  = "event"
	NamespaceID     = "id"
	NamespaceStatus = "status"
	StatusConsumed  = "consumed"
	StatusPublished = "published"
)

// ServiceConfig represents the configuration used to create a new event
// service.
type ServiceConfig struct {
	// Dependencies.
	IndexService      index.Service
	StorageCollection *storage.Collection

	// Settings.
	Kind string
}

// DefaultServiceConfig provides a default configuration to create a new event
// service by best effort.
func DefaultServiceConfig() ServiceConfig {
	var err error

	var indexService index.Service
	{
		indexConfig := index.DefaultServiceConfig()
		indexService, err = index.NewService(indexConfig)
		if err != nil {
			panic(err)
		}
	}

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
		IndexService:      indexService,
		StorageCollection: storageCollection,

		// Settings.
		Kind: "",
	}

	return config
}

// NewService creates a new configured event service.
func NewService(config ServiceConfig) (Service, error) {
	// Dependencies.
	if config.IndexService == nil {
		return nil, maskAnyf(invalidConfigError, "index service must not be empty")
	}
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
		index:   config.IndexService,
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
	index   index.Service
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
	result, err := s.storage.Event.PopFromList(s.queueKey())
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

func (s *service) Delete(event Event) error {
	// Remove the queued event.
	b, err := json.Marshal(event)
	if err != nil {
		return maskAny(err)
	}
	err = s.storage.Event.RemoveFromList(s.queueKey(), string(b))
	if err != nil {
		return maskAny(err)
	}

	// Remove the mapping between the labels and the event ID.
	labels, err := s.storage.Event.GetAllFromList(s.eventKey(event))
	if err != nil {
		return maskAny(err)
	}

	for _, l := range labels {
		err := s.storage.Event.RemoveFromList(s.labelKey(l), event.ID())
		if err != nil {
			return maskAny(err)
		}

		len, err := s.storage.Event.LengthOfList(s.labelKey(l))
		if err != nil {
			return maskAny(err)
		}
		if len != 0 {
			continue
		}

		// Make sure the list for mappings between a label and its associated event
		// IDs is removed as soon as there is no event ID anymore. This is important
		// for Service.ExistsAnyWithLabel.
		err = s.storage.Event.Remove(s.labelKey(l))
		if err != nil {
			return maskAny(err)
		}
	}

	// Remove the mapping between the event ID and the labels.
	err = s.storage.Event.Remove(s.eventKey(event))
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) ExistsAnyWithLabel(label string) (bool, error) {
	// We want to know if there does any event for a specific label exists.
	// Therefore we only have to check if a list for the given label exists at
	// all, because we remove all lists in case they are no longer used in
	// Service.Delete.
	ok, err := s.storage.Event.Exists(s.labelKey(label))
	if err != nil {
		return false, maskAny(err)
	}
	if ok {
		return true, nil
	}

	return false, nil
}

func (s *service) Publish(event Event) error {
	b, err := json.Marshal(event)
	if err != nil {
		return maskAny(err)
	}

	err = s.storage.Event.PushToList(s.queueKey(), string(b))
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) PublishWithLabels(event Event, labels ...string) error {
	if len(labels) == 0 {
		return maskAnyf(invalidExecutionError, "labels must not be empty")
	}

	err := s.Publish(event)
	if err != nil {
		return maskAny(err)
	}

	for _, l := range labels {
		err := s.storage.Event.PushToList(s.eventKey(event), l)
		if err != nil {
			return maskAny(err)
		}
	}

	for _, l := range labels {
		err := s.storage.Event.PushToList(s.labelKey(l), event.ID())
		if err != nil {
			return maskAny(err)
		}
	}

	return nil
}

func (s *service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}

func (s *service) eventKey(event Event) string {
	return fmt.Sprintf("event:%s", event.ID())
}

func (s *service) labelKey(label string) string {
	return fmt.Sprintf("label:%s", label)
}

func (s *service) queueKey() string {
	return fmt.Sprintf("queue:%s", s.kind)
}
