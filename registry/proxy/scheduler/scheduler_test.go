package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"
	"github.com/docker/distribution"
	distribution_context "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/libtrust"

	// "github.com/docker/distribution/registry/storage/driver/filesystem"
	"io"

	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/testutil"
)

func newRegistry(t *testing.T) (driver.StorageDriver, distribution.Namespace) {
	ctx := distribution_context.Background()
	k, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	localDriver := inmemory.New()
	localRegistry, err := storage.NewRegistry(ctx, localDriver, storage.BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), storage.EnableRedirect, storage.DisableDigestResumption, storage.Schema1SigningKey(k), storage.EnableSchema1)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	return localDriver, localRegistry
}

func populateRepo(ctx context.Context, t *testing.T, registry distribution.Namespace, repoName, tag string, blobsCount int) (reference.Canonical, []reference.Canonical) {
	blobsRefs := make([]reference.Canonical, 0, blobsCount)

	// Manifest that will contain information about the layers (blobs)
	m := schema1.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name: repoName,
		Tag:  tag,
	}

	// Parse repository name reference
	repoRef, err := reference.WithName(repoName)
	if err != nil {
		t.Fatalf("unable to parse reference for repository name %s: %v", repoName, err)
	}

	// Get the repository from the namespace
	repository, err := registry.Repository(ctx, repoRef)
	if err != nil {
		t.Fatalf("unexpected error getting repository '%s': %v", repoName, err)
	}

	// Create blobs and add them to the manifest
	for i := 0; i < blobsCount; i++ {
		wr, err := repository.Blobs(ctx).Create(ctx)
		if err != nil {
			t.Fatalf("unexpected error creating test upload for blob %d: %v", i, err)
		}

		// Generate a random tar file
		rs, blobDgst, err := testutil.CreateRandomTarFile()
		if err != nil {
			t.Fatalf("unexpected error generating test layer file for blob %d: %v", i, err)
		}

		// Copy data into the blob
		if _, err := io.Copy(wr, rs); err != nil {
			t.Fatalf("unexpected error copying to upload for blob %d: %v", i, err)
		}

		// Commit the blob
		if _, err := wr.Commit(ctx, distribution.Descriptor{Digest: blobDgst}); err != nil {
			t.Fatalf("unexpected error committing upload for blob %d: %v", i, err)
		}

		// Create a reference for the blob
		blobRef, err := reference.WithDigest(repoRef, blobDgst)
		if err != nil {
			t.Fatalf("failed to create blob reference for blob %d: %v", i, err)
		}

		blobsRefs = append(blobsRefs, blobRef)

		// Add blob information to the manifest
		m.FSLayers = append(m.FSLayers, schema1.FSLayer{BlobSum: blobDgst})
		m.History = append(m.History, schema1.History{V1Compatibility: fmt.Sprintf(`{"id": "%d"}`, i)})
	}

	// Generate a private key for signing the manifest
	pk, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		t.Fatalf("unexpected error generating private key: %v", err)
	}

	// Sign the manifest
	sm, err := schema1.Sign(&m, pk)
	if err != nil {
		t.Fatalf("error signing manifest: %v", err)
	}

	// Get the manifest service from the repository
	ms, err := repository.Manifests(ctx)
	if err != nil {
		t.Fatalf("error retrieving manifests from repository: %v", err)
	}

	// Store the manifest in the repository
	manifestDgst, err := ms.Put(ctx, sm)
	if err != nil {
		t.Fatalf("unexpected error putting manifest: %v", err)
	}

	// Create a reference for the manifest
	manifestRef, err := reference.WithDigest(repoRef, manifestDgst)
	if err != nil {
		t.Fatalf("failed to create manifest reference: %v", err)
	}

	return manifestRef, blobsRefs
}

func testRefs(t *testing.T) (reference.Reference, reference.Reference, reference.Reference) {
	ref1, err := reference.Parse("testrepo@sha256:aaaaeaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("could not parse reference: %v", err)
	}

	ref2, err := reference.Parse("testrepo@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatalf("could not parse reference: %v", err)
	}

	ref3, err := reference.Parse("testrepo@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatalf("could not parse reference: %v", err)
	}

	return ref1, ref2, ref3
}

func TestSchedule(t *testing.T) {
	driver, registry := newRegistry(t)

	ref1, ref2, ref3 := testRefs(t)
	timeUnit := time.Millisecond
	remainingRepos := map[string]bool{
		ref1.String(): true,
		ref2.String(): true,
		ref3.String(): true,
	}

	var mu sync.Mutex
	s := New(context.Background(), driver, registry, "/ttl")
	deleteFunc := func(repoName reference.Reference) error {
		if len(remainingRepos) == 0 {
			t.Fatalf("Incorrect expiry count")
		}
		_, ok := remainingRepos[repoName.String()]
		if !ok {
			t.Fatalf("Trying to remove nonexistent repo: %s", repoName)
		}
		t.Log("removing", repoName)
		mu.Lock()
		delete(remainingRepos, repoName.String())
		mu.Unlock()

		return nil
	}
	s.onBlobExpire = deleteFunc
	err := s.Start()
	if err != nil {
		t.Fatalf("Error starting ttlExpirationScheduler: %s", err)
	}

	s.add(ref1, 3*timeUnit, entryTypeBlob)
	s.add(ref2, 1*timeUnit, entryTypeBlob)

	func() {
		s.Lock()
		s.add(ref3, 1*timeUnit, entryTypeBlob)
		s.Unlock()

	}()

	// Ensure all repos are deleted
	<-time.After(50 * timeUnit)

	mu.Lock()
	defer mu.Unlock()
	if len(remainingRepos) != 0 {
		t.Fatalf("Repositories remaining: %#v", remainingRepos)
	}
}

func TestRestoreOld(t *testing.T) {
	driver, registry := newRegistry(t)

	ref1, ref2, _ := testRefs(t)
	remainingRepos := map[string]bool{
		ref1.String(): true,
		ref2.String(): true,
	}

	var wg sync.WaitGroup
	wg.Add(len(remainingRepos))
	var mu sync.Mutex
	deleteFunc := func(r reference.Reference) error {
		mu.Lock()
		defer mu.Unlock()
		if r.String() == ref1.String() && len(remainingRepos) == 2 {
			t.Errorf("ref1 should not be removed first")
		}
		_, ok := remainingRepos[r.String()]
		if !ok {
			t.Fatalf("Trying to remove nonexistent repo: %s", r)
		}
		delete(remainingRepos, r.String())
		wg.Done()
		return nil
	}

	timeUnit := time.Millisecond
	serialized, err := json.Marshal(&map[string]schedulerEntry{
		ref1.String(): {
			Expiry:    time.Now().Add(10 * timeUnit),
			Key:       ref1.String(),
			EntryType: 0,
		},
		ref2.String(): {
			Expiry:    time.Now().Add(-3 * timeUnit), // TTL passed, should be removed first
			Key:       ref2.String(),
			EntryType: 0,
		},
	})
	if err != nil {
		t.Fatalf("Error serializing test data: %s", err.Error())
	}

	ctx := context.Background()
	pathToStatFile := "/ttl"
	err = driver.PutContent(ctx, pathToStatFile, serialized)
	if err != nil {
		t.Fatal("Unable to write serialized data to fs")
	}
	s := New(context.Background(), driver, registry, "/ttl")
	s.OnBlobExpire(deleteFunc)
	err = s.Start()
	if err != nil {
		t.Fatalf("Error starting ttlExpirationScheduler: %s", err)
	}
	defer s.Stop()

	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(remainingRepos) != 0 {
		t.Fatalf("Repositories remaining: %#v", remainingRepos)
	}
}

func TestStopRestore(t *testing.T) {
	driver, registry := newRegistry(t)

	ref1, ref2, _ := testRefs(t)

	timeUnit := time.Millisecond
	remainingRepos := map[string]bool{
		ref1.String(): true,
		ref2.String(): true,
	}

	var mu sync.Mutex
	deleteFunc := func(r reference.Reference) error {
		mu.Lock()
		delete(remainingRepos, r.String())
		mu.Unlock()
		return nil
	}

	pathToStateFile := "/ttl"
	s := New(context.Background(), driver, registry, pathToStateFile)
	s.onBlobExpire = deleteFunc

	err := s.Start()
	if err != nil {
		t.Fatalf(err.Error())
	}
	s.add(ref1, 300*timeUnit, entryTypeBlob)
	s.add(ref2, 100*timeUnit, entryTypeBlob)

	// Start and stop before all operations complete
	// state will be written to fs
	s.Stop()
	time.Sleep(10 * time.Millisecond)

	// v2 will restore state from fs
	s2 := New(context.Background(), driver, registry, pathToStateFile)
	s2.onBlobExpire = deleteFunc
	err = s2.Start()
	if err != nil {
		t.Fatalf("Error starting v2: %s", err.Error())
	}

	<-time.After(500 * timeUnit)
	mu.Lock()
	defer mu.Unlock()
	if len(remainingRepos) != 0 {
		t.Fatalf("Repositories remaining: %#v", remainingRepos)
	}

}

func TestStateFromStorage(t *testing.T) {
	var mu sync.Mutex
	timeUnit := time.Millisecond
	pathToStatFile := "/ttl"
	saveRepositoryTTL := 10 * timeUnit

	saveRepositoryTTL, repositoryTTL = repositoryTTL, saveRepositoryTTL
	defer func() {
		saveRepositoryTTL, repositoryTTL = repositoryTTL, saveRepositoryTTL
	}()

	allManifests := map[reference.Canonical]struct{}{}
	allBlobs := map[reference.Canonical]struct{}{}

	ctx := context.Background()
	driver, registry := newRegistry(t)
	manifest1, blobs1 := populateRepo(ctx, t, registry, "test", "latest", 1)
	manifest2, blobs2 := populateRepo(ctx, t, registry, "a/b/c", "latest", 3)
	manifest3, blobs3 := populateRepo(ctx, t, registry, "c/d/e", "latest", 6)

	allManifests[manifest1] = struct{}{}
	allManifests[manifest2] = struct{}{}
	allManifests[manifest3] = struct{}{}
	for _, blob := range blobs1 {
		allBlobs[blob] = struct{}{}
	}
	for _, blob := range blobs2 {
		allBlobs[blob] = struct{}{}
	}
	for _, blob := range blobs3 {
		allBlobs[blob] = struct{}{}
	}

	s := New(context.Background(), driver, registry, pathToStatFile)
	s.OnBlobExpire(func(ref reference.Reference) error {
		t.Log("removing blob", ref.String())
		if len(allBlobs) == 0 {
			t.Fatalf("Incorrect expiry count")
		}
		var r reference.Canonical
		var ok bool
		if r, ok = ref.(reference.Canonical); !ok {
			t.Fatalf("unexpected reference type: %T", ref)
		}
		_, ok = allBlobs[r]
		if !ok {
			t.Fatalf("Trying to remove nonexistent blob: %s", r.String())
		}
		mu.Lock()
		delete(allBlobs, r)
		mu.Unlock()
		return nil
	})
	s.OnManifestExpire(func(ref reference.Reference) error {
		t.Log("removing manifest", ref.String())
		if len(allManifests) == 0 {
			t.Fatalf("Incorrect expiry count")
		}
		var r reference.Canonical
		var ok bool
		if r, ok = ref.(reference.Canonical); !ok {
			t.Fatalf("unexpected reference type: %T", ref)
		}
		_, ok = allManifests[r]
		if !ok {
			t.Fatalf("Trying to remove nonexistent manifests: %s", r.String())
		}
		mu.Lock()
		delete(allManifests, r)
		mu.Unlock()
		return nil
	})
	err := s.Start()
	if err != nil {
		t.Fatalf("Unable to start scheduler")
	}
	defer s.Stop()

	<-time.After(500 * timeUnit)

	mu.Lock()
	defer mu.Unlock()
	if len(allBlobs) != 0 {
		t.Fatalf("Blobs remaining: %#v", len(allBlobs))
	}
	if len(allManifests) != 0 {
		t.Fatalf("Manifests remaining: %#v", len(allManifests))
	}
}

func TestStateFromStorageRestoreOld(t *testing.T) {
	timeUnit := time.Millisecond
	pathToStatFile := "/ttl"
	saveRepositoryTTL := 10000 * timeUnit

	saveRepositoryTTL, repositoryTTL = repositoryTTL, saveRepositoryTTL
	defer func() {
		saveRepositoryTTL, repositoryTTL = repositoryTTL, saveRepositoryTTL
	}()

	ctx := context.Background()
	driver, registry := newRegistry(t)
	_, _ = populateRepo(ctx, t, registry, "test", "latest", 1)
	_, _ = populateRepo(ctx, t, registry, "a/b/c", "latest", 3)
	_, _ = populateRepo(ctx, t, registry, "c/d/e", "latest", 6)

	s1 := New(context.Background(), driver, registry, pathToStatFile)
	err := s1.Start()
	if err != nil {
		t.Fatalf("Unable to start scheduler")
	}

	time.Sleep(100 * timeUnit)
	s1.Stop()

	bytes1, err := driver.GetContent(ctx, pathToStatFile)
	if err != nil {
		t.Fatalf(err.Error())
	}

	s2 := New(context.Background(), driver, registry, pathToStatFile)
	err = s2.Start()
	if err != nil {
		t.Fatalf("Unable to start scheduler")
	}

	time.Sleep(100 * timeUnit)
	s2.Stop()

	bytes2, err := driver.GetContent(ctx, pathToStatFile)
	if err != nil {
		t.Fatalf(err.Error())
	}

	str1 := string(bytes1)
	str2 := string(bytes2)
	if str1 != str2 {
		t.Errorf("Expected bytes1 and bytes2 to be equal: %v and %v", str1, str2)
	}
}

func TestDoubleStart(t *testing.T) {
	driver, registry := newRegistry(t)

	s := New(context.Background(), driver, registry, "/ttl")
	err := s.Start()
	if err != nil {
		t.Fatalf("Unable to start scheduler")
	}
	err = s.Start()
	if err == nil {
		t.Fatalf("Scheduler started twice without error")
	}
}
