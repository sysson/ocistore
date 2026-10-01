package blobstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/oci"
	"github.com/sysson/ocistore/backend"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

var ErrObjectUnknown = errors.New("registry blob store: object unknown")

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type uploadState struct {
	Size   int64  `json:"size"`
	Chunks uint64 `json:"chunks"`
}

type Store struct {
	bucket  *blob.Bucket
	locksMu sync.Mutex
	locks   map[backend.UploadID]*uploadLock
}

type uploadLock struct {
	mu   sync.Mutex
	refs int
}

var _ backend.ContentStore = (*Store)(nil)

func Open(ctx context.Context, bucketURL string) (*Store, error) {
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("opening Go CDK blob bucket: %w", err)
	}
	return &Store{bucket: bucket, locks: make(map[backend.UploadID]*uploadLock)}, nil
}

func (s *Store) Close() error {
	return s.bucket.Close()
}

func (s *Store) Open(ctx context.Context, digest oci.Digest) (io.ReadCloser, error) {
	reader, err := s.bucket.NewReader(ctx, objectKey(digest), nil)
	if err != nil {
		return nil, mapObjectError(err)
	}
	return reader, nil
}

func (s *Store) OpenRange(ctx context.Context, digest oci.Digest, start, end int64) (io.ReadCloser, error) {
	if start < 0 || (end >= 0 && end < start) {
		return nil, fmt.Errorf("invalid content range [%d,%d)", start, end)
	}
	length := end - start
	if end < 0 {
		length = -1
	}
	reader, err := s.bucket.NewRangeReader(ctx, objectKey(digest), start, length, nil)
	if err != nil {
		return nil, mapObjectError(err)
	}
	return reader, nil
}

func (s *Store) Size(ctx context.Context, digest oci.Digest) (int64, error) {
	attributes, err := s.bucket.Attributes(ctx, objectKey(digest))
	if err != nil {
		return 0, mapObjectError(err)
	}
	return attributes.Size, nil
}

func (s *Store) PutBlob(ctx context.Context, digest oci.Digest, size int64, content io.Reader) error {
	if err := validateDigest(digest); err != nil {
		return err
	}
	if size < 0 {
		return oci.ErrSizeInvalid
	}
	id, err := randomID()
	if err != nil {
		return err
	}
	pending := "pending/" + id
	writer, err := s.bucket.NewWriter(ctx, pending, nil)
	if err != nil {
		return fmt.Errorf("opening staged blob writer: %w", err)
	}
	digester, err := digest.Algorithm().New()
	if err != nil {
		_ = writer.Close()
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return fmt.Errorf("creating content verifier: %w", err)
	}
	n, writeErr := io.Copy(writer, io.TeeReader(content, digester))
	closeErr := writer.Close()
	if writeErr != nil || closeErr != nil {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return errors.Join(writeErr, closeErr)
	}
	actual, err := digester.Digest()
	if err != nil {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return fmt.Errorf("calculating content digest: %w", err)
	}
	if n != size {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return fmt.Errorf("content size %d does not match expected size %d: %w", n, size, oci.ErrSizeInvalid)
	}
	if actual != digest {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return fmt.Errorf("content digest %s does not match expected digest %s: %w", actual, digest, oci.ErrDigestInvalid)
	}
	if err := s.bucket.Copy(ctx, objectKey(digest), pending, nil); err != nil {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return fmt.Errorf("publishing blob %s: %w", digest, err)
	}
	if err := s.bucket.Delete(ctx, pending); err != nil {
		return fmt.Errorf("removing staged blob %s: %w", digest, err)
	}
	return nil
}

func (s *Store) DeleteBlob(ctx context.Context, digest oci.Digest) error {
	if err := s.bucket.Delete(ctx, objectKey(digest)); err != nil && !isNotFound(err) {
		return fmt.Errorf("deleting blob %s: %w", digest, err)
	}
	return nil
}

// ListBlobs visits stored content, including objects missing from metadata.
func (s *Store) ListBlobs(ctx context.Context, visit func(oci.Digest) error) error {
	iterator := s.bucket.List(&blob.ListOptions{Prefix: "objects/"})
	for {
		object, err := iterator.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		parts := strings.Split(strings.TrimPrefix(object.Key, "objects/"), "/")
		if len(parts) != 2 {
			continue
		}
		digest := oci.Digest(parts[0] + ":" + parts[1])
		if digest.Validate() != nil {
			continue
		}
		if err := visit(digest); err != nil {
			return err
		}
	}
}

// CleanupStaged removes abandoned pending objects older than cutoff. Choose a
// cutoff before the start of any write that may still be active.
func (s *Store) CleanupStaged(ctx context.Context, cutoff time.Time) error {
	iterator := s.bucket.List(&blob.ListOptions{Prefix: "pending/"})
	var keys []string
	for {
		object, err := iterator.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("listing staged objects: %w", err)
		}
		if validID(strings.TrimPrefix(object.Key, "pending/")) && !object.ModTime.IsZero() && object.ModTime.Before(cutoff) {
			keys = append(keys, object.Key)
		}
	}
	for _, key := range keys {
		if err := s.bucket.Delete(ctx, key); err != nil && !isNotFound(err) {
			return fmt.Errorf("deleting staged object %s: %w", key, err)
		}
	}
	return nil
}

func (s *Store) StartUpload(ctx context.Context, id backend.UploadID) error {
	if !validID(string(id)) {
		return fmt.Errorf("invalid upload ID")
	}
	unlock := s.lockUpload(id)
	defer unlock()
	key := uploadIndexKey(id)
	if _, err := s.bucket.Attributes(ctx, key); err == nil {
		return nil
	} else if !isNotFound(err) {
		return fmt.Errorf("checking upload session: %w", err)
	}
	return s.writeUploadState(ctx, id, uploadState{})
}

func (s *Store) UploadSize(ctx context.Context, id backend.UploadID) (int64, error) {
	unlock := s.lockUpload(id)
	defer unlock()
	state, err := s.readUploadState(ctx, id)
	if err != nil {
		return 0, err
	}
	return state.Size, nil
}

func (s *Store) AppendUpload(ctx context.Context, id backend.UploadID, expectedOffset int64, content io.Reader) (int64, error) {
	unlock := s.lockUpload(id)
	defer unlock()
	state, err := s.readUploadState(ctx, id)
	if err != nil {
		return 0, err
	}
	if expectedOffset != state.Size {
		return state.Size, backend.ErrUploadOffset
	}
	if expectedOffset < 0 {
		return state.Size, backend.ErrUploadOffset
	}
	chunkKey := uploadChunkKey(id, state.Chunks)
	writer, err := s.bucket.NewWriter(ctx, chunkKey, nil)
	if err != nil {
		return state.Size, fmt.Errorf("opening upload chunk writer: %w", err)
	}
	n, writeErr := io.Copy(writer, content)
	closeErr := writer.Close()
	if writeErr != nil || closeErr != nil {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), chunkKey)
		return state.Size, errors.Join(writeErr, closeErr)
	}
	if n > int64(^uint64(0)>>1)-state.Size {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), chunkKey)
		return state.Size, oci.ErrSizeInvalid
	}
	state.Size += n
	state.Chunks++
	if err := s.writeUploadState(ctx, id, state); err != nil {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), chunkKey)
		state.Size -= n
		state.Chunks--
		return state.Size, fmt.Errorf("persisting upload offset: %w", err)
	}
	return state.Size, nil
}

func (s *Store) CommitUpload(ctx context.Context, id backend.UploadID, digest oci.Digest, size int64) error {
	if err := validateDigest(digest); err != nil {
		return err
	}
	if size < 0 {
		return oci.ErrSizeInvalid
	}
	unlock := s.lockUpload(id)
	defer unlock()
	state, err := s.readUploadState(ctx, id)
	if err != nil {
		return err
	}
	if state.Size != size {
		return fmt.Errorf("upload size %d does not match expected size %d: %w", state.Size, size, oci.ErrSizeInvalid)
	}
	digester, err := digest.Algorithm().New()
	if err != nil {
		return fmt.Errorf("creating upload verifier: %w", err)
	}
	for i := uint64(0); i < state.Chunks; i++ {
		reader, err := s.bucket.NewReader(ctx, uploadChunkKey(id, i), nil)
		if err != nil {
			return fmt.Errorf("opening upload chunk %d: %w", i, err)
		}
		_, readErr := io.Copy(digester, reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
	}
	actual, err := digester.Digest()
	if err != nil {
		return fmt.Errorf("calculating upload digest: %w", err)
	}
	if digester.Size() != size {
		return fmt.Errorf("staged bytes %d do not match expected size %d: %w", digester.Size(), size, oci.ErrSizeInvalid)
	}
	if actual != digest {
		return fmt.Errorf("upload digest %s does not match expected digest %s: %w", actual, digest, oci.ErrDigestInvalid)
	}

	pendingID, err := randomID()
	if err != nil {
		return err
	}
	pending := "pending/" + pendingID
	writer, err := s.bucket.NewWriter(ctx, pending, nil)
	if err != nil {
		return fmt.Errorf("opening staged upload writer: %w", err)
	}
	for i := uint64(0); i < state.Chunks; i++ {
		reader, err := s.bucket.NewReader(ctx, uploadChunkKey(id, i), nil)
		if err != nil {
			_ = writer.Close()
			_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
			return fmt.Errorf("opening upload chunk %d: %w", i, err)
		}
		_, copyErr := io.Copy(writer, reader)
		closeErr := reader.Close()
		if copyErr != nil || closeErr != nil {
			_ = writer.Close()
			_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
			return errors.Join(copyErr, closeErr)
		}
	}
	if err := writer.Close(); err != nil {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return fmt.Errorf("closing staged upload: %w", err)
	}
	if err := s.bucket.Copy(ctx, objectKey(digest), pending, nil); err != nil {
		_ = s.bucket.Delete(context.WithoutCancel(ctx), pending)
		return fmt.Errorf("publishing upload %s: %w", digest, err)
	}
	if err := s.bucket.Delete(ctx, pending); err != nil {
		return fmt.Errorf("removing staged upload object: %w", err)
	}
	return nil
}

func (s *Store) CancelUpload(ctx context.Context, id backend.UploadID) error {
	unlock := s.lockUpload(id)
	defer unlock()
	state, err := s.readUploadState(ctx, id)
	if err != nil {
		if errors.Is(err, backend.ErrUploadUnknown) {
			return nil
		}
		return err
	}
	for i := uint64(0); i < state.Chunks; i++ {
		if err := s.bucket.Delete(ctx, uploadChunkKey(id, i)); err != nil && !isNotFound(err) {
			return fmt.Errorf("deleting upload chunk %d: %w", i, err)
		}
	}
	if err := s.bucket.Delete(ctx, uploadIndexKey(id)); err != nil && !isNotFound(err) {
		return fmt.Errorf("deleting upload index: %w", err)
	}
	return nil
}

func (s *Store) readUploadState(ctx context.Context, id backend.UploadID) (uploadState, error) {
	if !validID(string(id)) {
		return uploadState{}, backend.ErrUploadUnknown
	}
	data, err := s.bucket.ReadAll(ctx, uploadIndexKey(id))
	if err != nil {
		if isNotFound(err) {
			return uploadState{}, backend.ErrUploadUnknown
		}
		return uploadState{}, fmt.Errorf("reading upload index: %w", err)
	}
	var state uploadState
	if err := json.Unmarshal(data, &state); err != nil {
		return uploadState{}, fmt.Errorf("decoding upload index: %w", err)
	}
	if state.Size < 0 {
		return uploadState{}, fmt.Errorf("upload index has a negative offset")
	}
	return state, nil
}

func (s *Store) writeUploadState(ctx context.Context, id backend.UploadID, state uploadState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	writer, err := s.bucket.NewWriter(ctx, uploadIndexKey(id), nil)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func (s *Store) lockUpload(id backend.UploadID) func() {
	s.locksMu.Lock()
	lock := s.locks[id]
	if lock == nil {
		lock = &uploadLock{}
		s.locks[id] = lock
	}
	lock.refs++
	s.locksMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.locksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.locks, id)
		}
		s.locksMu.Unlock()
	}
}

func objectKey(digest oci.Digest) string {
	return "objects/" + digest.Algorithm().String() + "/" + digest.Encoded()
}

func uploadIndexKey(id backend.UploadID) string {
	return "uploads/" + string(id) + "/index.json"
}

func uploadChunkKey(id backend.UploadID, chunk uint64) string {
	return "uploads/" + string(id) + "/chunks/" + strconv.FormatUint(chunk, 10)
}

func validateDigest(digest oci.Digest) error {
	if err := digest.Validate(); err != nil {
		return fmt.Errorf("invalid digest: %w: %v", oci.ErrDigestInvalid, err)
	}
	return nil
}

func mapObjectError(err error) error {
	if isNotFound(err) {
		return fmt.Errorf("%w: %v", ErrObjectUnknown, err)
	}
	return err
}

func isNotFound(err error) bool {
	return gcerrors.Code(err) == gcerrors.NotFound
}

func validID(id string) bool {
	return idPattern.MatchString(id)
}

func randomID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating random identifier: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
