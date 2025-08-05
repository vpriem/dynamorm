package dynamorm

import "fmt"

// ErrEntityNotFound is returned error by storage.Get when an entity cannot be found in DynamoDB.
var ErrEntityNotFound = fmt.Errorf("entity not found")

// ErrIndexOutOfRange returned by Query.Decode when attempting to access an element at an index
// that is outside the valid range of the query results.
var ErrIndexOutOfRange = fmt.Errorf("index out of range")
