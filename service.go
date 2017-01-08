package event

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenk/backoff"
	"github.com/the-anna-project/context"
	"github.com/the-anna-project/instrumentor"
	"github.com/the-anna-project/storage"
)

const (
	// KindActivator represents the event service responsible for managing
	// activator events.
	KindActivator = "activator"
	// KindNetwork represents the event service responsible for managing
	// network events.
	KindNetwork = "network"
	// NamespaceDefault represents the default namespace in which signals can be
	// put that are not supposed to be queued in any custom namespace.
	NamespaceDefault = "default"
	// LabelWildcard represents a wildcard label which can be used to consume
	// events associated with all labels using Service.Search.
	LabelWildcard = "*"
)

// ServiceConfig represents the configuration used to create a new event
// service.
type ServiceConfig struct {
	// Dependencies.
	BackoffService         func() Backoff
	InstrumentorCollection *instrumentor.Collection
	StorageCollection      *storage.Collection

	// Settings.
	Kind string
}

// DefaultServiceConfig provides a default configuration to create a new event
// service by best effort.
func DefaultServiceConfig() ServiceConfig {
	var err error

	var backoffService func() Backoff
	{
		backoffService = func() Backoff {
			return &backoff.StopBackOff{}
		}
	}

	var instrumentorCollection *instrumentor.Collection
	{
		instrumentorConfig := instrumentor.DefaultCollectionConfig()
		instrumentorCollection, err = instrumentor.NewCollection(instrumentorConfig)
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
		BackoffService:         backoffService,
		InstrumentorCollection: instrumentorCollection,
		StorageCollection:      storageCollection,

		// Settings.
		Kind: "",
	}

	return config
}

// NewService creates a new configured event service.
func NewService(config ServiceConfig) (Service, error) {
	// Dependencies.
	if config.BackoffService == nil {
		return nil, maskAnyf(invalidConfigError, "backoff service must not be empty")
	}
	if config.InstrumentorCollection == nil {
		return nil, maskAnyf(invalidConfigError, "instrumentor collection must not be empty")
	}
	if config.StorageCollection == nil {
		return nil, maskAnyf(invalidConfigError, "storage collection must not be empty")
	}

	// Settings.
	if config.Kind == "" {
		return nil, maskAnyf(invalidConfigError, "kind must not be empty")
	}
	if config.Kind != KindActivator && config.Kind != KindNetwork {
		return nil, maskAnyf(invalidConfigError, "kind must be %s or %s", KindActivator, KindNetwork)
	}

	newService := &service{
		// Dependencies.
		backoff:      config.BackoffService,
		instrumentor: config.InstrumentorCollection,
		storage:      config.StorageCollection,

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
	backoff      func() Backoff
	instrumentor *instrumentor.Collection
	storage      *storage.Collection

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

func (s *service) Create(ctx context.Context, event Event, labels ...string) error {
	namespace := s.namespaceFromLabels(labels...)
	if namespace == LabelWildcard {
		return maskAnyf(invalidExecutionError, "wildcard namespace must only be used for Service.Search")
	}

	// Register the namespace in the lookup table. Duplicated elements will be
	// ignored so we can simply fire and forget.
	err := s.storage.Event.PushToSet(s.tableKey(), namespace)
	if err != nil {
		return maskAny(err)
	}

	// Publish the event ID in its namespaced queue.
	err = s.storage.Event.PushToList(s.namespaceKey(namespace), event.ID())
	if err != nil {
		return maskAny(err)
	}

	// Store the event payload.
	err = s.storage.Event.Set(s.eventKey(event.ID()), event.Payload())
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Delete(ctx context.Context, event Event, labels ...string) error {
	namespace := s.namespaceFromLabels(labels...)
	if namespace == LabelWildcard {
		return maskAnyf(invalidExecutionError, "wildcard namespace must only be used for Service.Search")
	}

	err := s.storage.Event.Remove(s.eventKey(event.ID()))
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) ExistsAny(ctx context.Context, labels ...string) (bool, error) {
	namespace := s.namespaceFromLabels(labels...)
	if namespace == LabelWildcard {
		return false, maskAnyf(invalidExecutionError, "wildcard namespace must only be used for Service.Search")
	}

	// We want to know if there does any event associated with a specific set of
	// labels exists. Therefore we only have to check if a list for our namespace
	// exists at all, because the underlying list is automatically removed by the
	// storage service in case there are no longer events queued within it.
	ok, err := s.storage.Event.Exists(s.namespaceKey(namespace))
	if err != nil {
		return false, maskAny(err)
	}
	if ok {
		return true, nil
	}

	return false, nil
}

func (s *service) Limit(ctx context.Context, max int, labels ...string) error {
	if max < 1 {
		return maskAnyf(invalidExecutionError, "max must be 1 or greater")
	}

	namespace := s.namespaceFromLabels(labels...)
	if namespace == LabelWildcard {
		return maskAnyf(invalidExecutionError, "wildcard namespace must only be used for Service.Search")
	}

	err := s.storage.Event.TrimEndOfList(s.namespaceKey(namespace), max)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Search(ctx context.Context, labels ...string) (Event, error) {
	namespace := s.namespaceFromLabels(labels...)

	var event Event
	var err error
	action := func() error {

		// If the caller wants to consume any event using the wildcard label it
		// might happen that there is no event at all. Then a not found error is
		// received. This causes the retry action to fail. The failed action is
		// retried based on the rules of the backoff service and its retry capacity.
		if namespace == LabelWildcard {
			namespace, err = s.storage.Event.GetRandomFromSet(s.tableKey())
			if err != nil {
				return maskAny(err)
			}
		}

		eventID, err := s.storage.Event.PopFromList(s.namespaceKey(namespace))
		if err != nil {
			return maskAny(err)
		}

		ok, err := s.ExistsAny(ctx, labels...)
		if err != nil {
			return maskAny(err)
		}
		if !ok {
			err := s.storage.Event.RemoveFromSet(s.tableKey(), namespace)
			if err != nil {
				return maskAny(err)
			}
		}

		// Fetching the actually queued event might fail if the caller already
		// deleted the event. Then we receive a not found error and the failed retry
		// action will be retried by the backoff service, depending of its
		// configured rules and retry budged.
		rawEvent, err := s.storage.Event.Get(s.eventKey(eventID))
		if err != nil {
			return maskAny(err)
		}

		newEvent, err := New(DefaultConfig())
		if err != nil {
			return maskAny(err)
		}
		err = json.Unmarshal([]byte(rawEvent), &newEvent)
		if err != nil {
			return maskAny(err)
		}
		event = newEvent

		return nil
	}

	// TODO use the proper backoff service
	err = backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("Search", action), s.backoff(), s.retryNotifier)
	if err != nil {
		return nil, maskAny(err)
	}

	return event, nil
}

func (s *service) SearchAll(ctx context.Context, labels ...string) ([]Event, error) {
	namespace := s.namespaceFromLabels(labels...)
	if namespace == LabelWildcard {
		return nil, maskAnyf(invalidExecutionError, "wildcard namespace must only be used for Service.Search")
	}

	// In case there is not any event queued or the list does not exist at all
	// (which is implicitely the same), we return a not found error. The
	// underlying storage implementation would return an empty list, but we do not
	// want this for the event service interface. That way we have a clear
	// distinction between a successful and a failed operation.
	ok, err := s.ExistsAny(ctx, labels...)
	if err != nil {
		return nil, maskAny(err)
	}
	if !ok {
		return nil, maskAny(notFoundError)
	}

	eventIDs, err := s.storage.Event.GetAllFromList(s.namespaceKey(namespace))
	if err != nil {
		return nil, maskAny(err)
	}

	var events []Event

	for _, eventID := range eventIDs {
		rawEvent, err := s.storage.Event.Get(s.eventKey(eventID))
		if err != nil {
			return nil, maskAny(err)
		}

		newEvent, err := New(DefaultConfig())
		if err != nil {
			return nil, maskAny(err)
		}
		err = json.Unmarshal([]byte(rawEvent), &newEvent)
		if err != nil {
			return nil, maskAny(err)
		}

		events = append(events, newEvent)
	}

	return events, nil
}

func (s *service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}

func (s *service) WriteAll(ctx context.Context, events []Event, labels ...string) error {
	namespace := s.namespaceFromLabels(labels...)
	if namespace == LabelWildcard {
		return maskAnyf(invalidExecutionError, "wildcard namespace must only be used for Service.Search")
	}

	for {
		ok, err := s.ExistsAny(ctx, labels...)
		if err != nil {
			return maskAny(err)
		}
		if !ok {
			break
		}
		e, err := s.Search(ctx, labels...)
		if err != nil {
			return maskAny(err)
		}
		err = s.Delete(ctx, e, labels...)
		if err != nil {
			return maskAny(err)
		}
	}

	for _, e := range events {
		err := s.Create(ctx, e, labels...)
		if err != nil {
			return maskAny(err)
		}
	}

	return nil
}

func (s *service) eventKey(eventID string) string {
	return fmt.Sprintf("service:event:kind:%s:event:%s", s.kind, eventID)
}

// redis list
// holding events
func (s *service) namespaceKey(namespace string) string {
	return fmt.Sprintf("service:event:kind:%s:namespace:%s", s.kind, namespace)
}

func (s *service) namespaceFromLabels(labels ...string) string {
	sort.Strings(labels)

	namespace := strings.Join(labels, "")

	return namespace
}

// TODO emit metrics in proper backoff service
func (s *service) retryNotifier(err error, d time.Duration) {
	//s.logger.Log("error", fmt.Sprintf("%#v", maskAny(err)))
}

// redis set
// holding all namespaces
// random member
// check existence
func (s *service) tableKey() string {
	return fmt.Sprintf("service:event:kind:%s:table", s.kind)
}
