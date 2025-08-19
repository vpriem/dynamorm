package dynamorm

import (
	"errors"
	"fmt"
)

// ErrEntityNotFound is returned by Storage.Get when an entity with the given PK/SK
// does not exist in DynamoDB.
var ErrEntityNotFound = errors.New("entity not found")

// ErrEntityPkNotSet is returned when an entity's partition key (PK) is empty
// while an operation requires it (e.g., Storage.Save/Get/Update/Remove).
var ErrEntityPkNotSet = errors.New("entity PK not set")

// ErrEntitySkNotSet is returned when an entity's sort key (SK) is empty
// while an operation requires it (e.g., Storage.Save/Get/Update/Remove).
var ErrEntitySkNotSet = errors.New("entity SK not set")

// ErrEntityEncode is returned when the Encoder fails to convert an entity into a
// DynamoDB attribute map (e.g., during Storage.Save).
var ErrEntityEncode = errors.New("failed to encode entity")

// ErrEntityDecode is returned when the Decoder fails to convert a DynamoDB item
// into the target entity (e.g., during Storage.Get).
var ErrEntityDecode = errors.New("failed to decode entity")

// ErrEntityBeforeSave is returned when Entity.BeforeSave returns an error during
// a save operation (e.g., Storage.Save).
var ErrEntityBeforeSave = errors.New("failed to execute entity.BeforeSave")

// ErrBatch is returned by batch write operations when DynamoDB responds with
// unprocessed items that could not be written (affecting BatchSave/BatchRemove).
var ErrBatch = errors.New("failed to process all items in batch")

// ErrClient is used to wrap errors returned by the underlying DynamoDB client.
var ErrClient = errors.New("client error")

// ErrIndexOutOfRange is returned by Query.Decode, Query.First, and Query.Last
// when the requested item is outside the bounds of the current result set,
// including when there are no items.
var ErrIndexOutOfRange = errors.New("index out of range")

// NewClientError wraps an error returned by the underlying DynamoDB client
// in a ClientError.
func NewClientError(err error) *ClientError {
	return &ClientError{err}
}

// ClientError represents errors originating from the DynamoDB client. It wraps
// the original client error (accessible via errors.Unwrap / errors.As).
type ClientError struct {
	err error
}

// Error returns a human-readable message.
func (e *ClientError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%v: %v", ErrClient, e.err)
	}
	return ErrClient.Error()
}

// Unwrap exposes the wrapped client error so callers can use errors.As oto inspect the underlying AWS error type.
func (e *ClientError) Unwrap() error {
	return e.err
}

// Is makes ClientError match ErrClient when used with errors.Is, allowing
// callers to detect client-originated errors without losing the underlying type.
func (e *ClientError) Is(target error) bool {
	return target == ErrClient
}
