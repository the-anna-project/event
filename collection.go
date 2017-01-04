package event

import (
	"sync"

	"github.com/the-anna-project/storage"
)

// CollectionConfig represents the configuration used to create a new event
// collection.
type CollectionConfig struct {
	// Dependencies.
	StorageCollection *storage.Collection
}

// DefaultCollectionConfig provides a default configuration to create a new
// event collection by best effort.
func DefaultCollectionConfig() CollectionConfig {
	var err error

	var storageCollection *storage.Collection
	{
		storageConfig := storage.DefaultCollectionConfig()
		storageCollection, err = storage.NewCollection(storageConfig)
		if err != nil {
			panic(err)
		}
	}

	config := CollectionConfig{
		// Dependencies.
		StorageCollection: storageCollection,
	}

	return config
}

// NewCollection creates a new configured event Collection.
func NewCollection(config CollectionConfig) (*Collection, error) {
	// Dependencies.
	if config.StorageCollection == nil {
		return nil, maskAnyf(invalidConfigError, "storage collection must not be empty")
	}

	var err error

	var activatorService Service
	{
		activatorConfig := DefaultServiceConfig()
		activatorConfig.Kind = KindActivator
		activatorConfig.StorageCollection = config.StorageCollection
		activatorService, err = NewService(activatorConfig)
		if err != nil {
			return nil, maskAny(err)
		}
	}

	var networkService Service
	{
		networkConfig := DefaultServiceConfig()
		networkConfig.Kind = KindNetwork
		networkConfig.StorageCollection = config.StorageCollection
		networkService, err = NewService(networkConfig)
		if err != nil {
			return nil, maskAny(err)
		}
	}

	newCollection := &Collection{
		// Internals.
		bootOnce:     sync.Once{},
		shutdownOnce: sync.Once{},

		Activator: activatorService,
		Network:   networkService,
	}

	return newCollection, nil
}

// Collection is the object bundling all services.
type Collection struct {
	// Internals.
	bootOnce     sync.Once
	shutdownOnce sync.Once

	Activator Service
	Network   Service
}

func (c *Collection) Boot() {
	c.bootOnce.Do(func() {
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			c.Activator.Boot()
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			c.Network.Boot()
			wg.Done()
		}()

		wg.Wait()
	})
}

func (c *Collection) Shutdown() {
	c.shutdownOnce.Do(func() {
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			c.Activator.Shutdown()
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			c.Network.Shutdown()
			wg.Done()
		}()

		wg.Wait()
	})
}
