package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/distribution/reference"
	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

const (
	isTTLSetterFinished = "isTTLSetterFinished"
	repositoryTTL       = 24 * 7 * time.Hour // TTL for repository objects (1 week)
)

// New creates a new TTLExpirationSetter instance
func NewTTLExpirationSetter(
	ctx context.Context,
	registry distribution.Namespace,
	scheduler TTLExpirationScheduler,
	opStateInstance *OperationsStateInstance,
) *TTLExpirationSetter {
	return &TTLExpirationSetter{
		ctx:             ctx,
		opStateInstance: opStateInstance,
		scheduler:       scheduler,
		registry:        registry,
	}
}

// TTLExpirationScheduler defines the interface for scheduling TTL expiration
type TTLExpirationScheduler interface {
	AddBlob(blobRef reference.Canonical, ttl time.Duration) error
	AddManifest(manifestRef reference.Canonical, ttl time.Duration) error
	SaveState() error
}

// TTLExpirationSetter is responsible for setting expiration times for cached objects
type TTLExpirationSetter struct {
	ctx             context.Context
	opStateInstance *OperationsStateInstance
	scheduler       TTLExpirationScheduler
	registry        distribution.Namespace // Local registry functionality
}

// Start begins the TTL setting process
func (ttlset *TTLExpirationSetter) Start() error {
	// Retrieve the current state of whether the TTL setting task is finished
	opState := ttlset.opStateInstance.GetOperationsState(isTTLSetterFinished)
	isFinished := true

	// If no previous state is available, assume the task hasn't been completed
	if opState == nil {
		isFinished = false
	} else {
		var ok bool
		isFinished, ok = opState.(bool)
		if !ok {
			return fmt.Errorf("cannot convert %T to bool", opState)
		}
	}

	// If TTL setting has already been completed, exit early
	if isFinished {
		return nil
	}

	dcontext.GetLogger(ttlset.ctx).Infof("Starting cached object TTL expiration setter...")

	// Launch the TTL setting process in a separate goroutine
	go func() {
		for {
			err := ttlset.setTTL()
			if err != nil {
				dcontext.GetLogger(ttlset.ctx).Error(err)
				time.Sleep(10 * time.Second) // Retry after a delay in case of failure
			} else {
				break
			}
		}

		// Mark the operation as complete
		err := ttlset.opStateInstance.SetOperationsState(isTTLSetterFinished, true)
		if err != nil {
			dcontext.GetLogger(ttlset.ctx).Errorf("failed to update operation state: %v", err)
		}
	}()
	dcontext.GetLogger(ttlset.ctx).Infof("cached object TTL expiration setter is done")
	return nil
}

// setTTL sets expiration (TTL) for manifests and blobs in the repository
func (ttlset *TTLExpirationSetter) setTTL() error {
	// Convert registry to a RepositoryEnumerator to iterate through repositories
	repositoryEnumerator, ok := ttlset.registry.(distribution.RepositoryEnumerator)
	if !ok {
		return fmt.Errorf("unable to convert Namespace to RepositoryEnumerator")
	}

	// Sets to track manifests and blobs that will have TTLs assigned
	manifestRefSet := make(map[reference.Canonical]struct{})
	blobRefSet := make(map[reference.Canonical]struct{})

	// Iterate through all repositories
	err := repositoryEnumerator.Enumerate(ttlset.ctx, func(repoName string) error {
		named, err := reference.WithName(repoName)
		if err != nil {
			return fmt.Errorf("failed to parse repo name %s: %v", repoName, err)
		}

		// Retrieve the repository
		repository, err := ttlset.registry.Repository(ttlset.ctx, named)
		if err != nil {
			return fmt.Errorf("failed to construct repository: %v", err)
		}

		// Retrieve the manifest service for the repository
		manifestService, err := repository.Manifests(ttlset.ctx)
		if err != nil {
			return fmt.Errorf("failed to construct manifest service: %v", err)
		}

		// Convert the manifest service to a ManifestEnumerator
		manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
		if !ok {
			return fmt.Errorf("unable to convert ManifestService into ManifestEnumerator")
		}

		// Enumerate through all manifests in the repository
		err = manifestEnumerator.Enumerate(ttlset.ctx, func(dgst digest.Digest) error {
			// Add the manifest reference to the tracking set
			manifestRef, err := reference.WithDigest(named, dgst)
			if err != nil {
				return fmt.Errorf("failed to create manifest reference: %v", err)
			}
			manifestRefSet[manifestRef] = struct{}{}

			// Retrieve the manifest data
			manifest, err := manifestService.Get(ttlset.ctx, dgst)
			if err != nil {
				return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
			}

			// Add all blobs from the manifest's references to the blob tracking set
			descriptors := manifest.References()
			for _, descriptor := range descriptors {
				blobRef, err := reference.WithDigest(named, descriptor.Digest)
				if err != nil {
					return fmt.Errorf("failed to create blob reference: %v", err)
				}
				blobRefSet[blobRef] = struct{}{}
			}
			return nil
		})

		// Handle non-existent paths (e.g., unfinished uploads or deleted content)
		if err != nil {
			if _, ok := err.(driver.PathNotFoundError); !ok {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to mark manifests and blobs: %v", err)
	}

	// Schedule TTL for all manifests
	for manifestRef := range manifestRefSet {
		err = ttlset.scheduler.AddManifest(manifestRef, repositoryTTL)
		if err != nil {
			return fmt.Errorf("failed to add manifest to TTL scheduler: %v", err)
		}
	}

	// Schedule TTL for all blobs
	for blobRef := range blobRefSet {
		err = ttlset.scheduler.AddBlob(blobRef, repositoryTTL)
		if err != nil {
			return fmt.Errorf("failed to add blob to TTL scheduler: %v", err)
		}
	}

	// Persist the scheduler state to ensure data is saved
	return ttlset.scheduler.SaveState()
}
