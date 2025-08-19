package dynamorm

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// QueryInterface provides an interface to handle DynamoDB query results.
// It supports automatic pagination when iterating through results using Next().
type QueryInterface interface {
	// Count returns the number of items of the current the query result
	Count() int32
	// ScannedCount returns the number of items evaluated of the current the query result
	ScannedCount() int32
	// First decodes the first item in the current result set into the provided entity.
	// Returns ErrIndexOutOfRange if there are no items.
	First(Entity) error
	// Last decodes the last item in the current result set into the provided entity.
	// Returns ErrIndexOutOfRange if there are no items.
	Last(Entity) error
	// Next advances the iterator to the next item.
	// Returns true if there is a next item available, false otherwise.
	Next() bool
	// NextPage fetches the next page of results using the LastEvaluatedKey.
	// Returns true if there are more results to be fetched, false otherwise.
	// Any error encountered can be retrieved via Error().
	NextPage(context.Context) bool
	// Error returns the last error encountered during pagination.
	Error() error
	// Reset resets the iterator to the beginning
	Reset()
	// Decode decodes the current item into the provided interface
	Decode(Entity) error
}

// Query implements the QueryInterface for handling DynamoDB query results.
// It provides methods for iterating through query results, automatic pagination,
// and decoding items into Go structs.
type Query struct {
	client  DynamoDB
	query   *dynamodb.QueryInput
	scan    *dynamodb.ScanInput
	output  *Output
	decoder DecoderInterface
	index   int
	paged   bool
	err     error
}

// NewQuery creates a new Query instance from the query input and output.
func NewQuery(client DynamoDB, query *dynamodb.QueryInput, scan *dynamodb.ScanInput, output *Output, decoder DecoderInterface) *Query {
	if query == nil && scan == nil {
		query = &dynamodb.QueryInput{}
	}
	if output == nil {
		output = &Output{}
	}
	if decoder == nil {
		decoder = DefaultDecoder()
	}

	return &Query{
		client:  client,
		query:   query,
		scan:    scan,
		output:  output,
		decoder: decoder,
	}
}

func (q *Query) Count() int32 {
	return q.output.Count
}

func (q *Query) ScannedCount() int32 {
	return q.output.ScannedCount
}

func (q *Query) First(e Entity) error {
	if len(q.output.Items) == 0 {
		return ErrIndexOutOfRange
	}

	item := q.output.Items[0]
	if err := q.decoder.Decode(item, e); err != nil {
		return fmt.Errorf("%w: %v", ErrEntityDecode, err)
	}
	return nil
}

func (q *Query) Last(e Entity) error {
	length := len(q.output.Items)
	if length == 0 {
		return ErrIndexOutOfRange
	}

	item := q.output.Items[length-1]
	if err := q.decoder.Decode(item, e); err != nil {
		return fmt.Errorf("%w: %v", ErrEntityDecode, err)
	}
	return nil
}

func (q *Query) Next() bool {
	q.index++
	return q.index <= len(q.output.Items)
}

func (q *Query) NextPage(ctx context.Context) bool {
	// On first call, serve the current page without fetching
	if !q.paged {
		q.paged = true
		return true
	}

	if q.output.LastEvaluatedKey == nil {
		return false
	}

	if q.scan != nil {
		q.scan.ExclusiveStartKey = q.output.LastEvaluatedKey
		out, err := q.client.Scan(ctx, q.scan)
		if err != nil {
			q.err = NewClientError(err)
			return false
		}
		q.output = NewOutputFromScanOutput(out)

	} else {
		q.query.ExclusiveStartKey = q.output.LastEvaluatedKey
		out, err := q.client.Query(ctx, q.query)
		if err != nil {
			q.err = NewClientError(err)
			return false
		}
		q.output = NewOutputFromQueryOutput(out)
	}

	q.Reset()
	return true
}

func (q *Query) Reset() {
	q.index = 0
	q.err = nil
}

func (q *Query) Decode(e Entity) error {
	if q.index <= 0 || q.index > len(q.output.Items) {
		return ErrIndexOutOfRange
	}

	if err := q.decoder.Decode(q.output.Items[q.index-1], e); err != nil {
		return fmt.Errorf("%w: %v", ErrEntityDecode, err)
	}

	return nil
}

func (q *Query) Error() error {
	return q.err
}
