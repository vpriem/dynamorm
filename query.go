package dynamorm

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// QueryInterface provides an interface to handle DynamoDB query results.
// It supports automatic pagination when iterating through results using Next().
type QueryInterface interface {
	// Count returns the number of items of the last the query result
	Count() int32
	// ScannedCount returns the number of items evaluated, before any QueryFilter is applied
	ScannedCount() int32
	// TotalCount returns the computed number of items from all query results
	TotalCount() int
	// First retrieves the first matching entity from the query results
	// Optional FindCondition parameters can be provided to filter the results
	First(Entity, ...FindCondition) error
	// Next advances the iterator to the next item.
	// If the end of current results is reached,
	// it will fetch the next batch of results automatically using the provided context.
	// Returns true if there is a next item available, false otherwise.
	Next(context.Context) bool
	// Reset resets the iterator to the beginning
	// This allows reiterating through the items without fetching data again
	Reset()
	// Decode decodes the current item into the provided interface
	Decode(Entity) error
	// Error return the last iteration error if any
	Error() error
}

// Query implements the QueryInterface for handling DynamoDB query results.
type Query struct {
	client  DynamoDB
	input   *dynamodb.QueryInput
	output  *dynamodb.QueryOutput
	decoder DecoderInterface
	index   int
	items   []map[string]types.AttributeValue
	mutex   sync.Mutex
	error   error
}

func NewQuery(client DynamoDB, input *dynamodb.QueryInput, output *dynamodb.QueryOutput, decoder DecoderInterface) *Query {
	if input == nil {
		input = &dynamodb.QueryInput{}
	}
	if output == nil {
		output = &dynamodb.QueryOutput{}
	}
	if decoder == nil {
		decoder = DefaultDecoder()
	}

	return &Query{
		client:  client,
		input:   input,
		output:  output,
		decoder: decoder,
		items:   output.Items,
	}
}

func (q *Query) isLastPage() bool {
	return q.output.LastEvaluatedKey == nil
}

func (q *Query) Count() int32 {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.output.Count
}

func (q *Query) ScannedCount() int32 {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.output.ScannedCount
}

func (q *Query) TotalCount() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return len(q.items)
}

func (q *Query) First(e Entity, conditions ...FindCondition) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	var foundItem map[string]types.AttributeValue

	if len(conditions) == 0 {
		if len(q.items) > 0 {
			foundItem = q.items[0]
		}
	} else {
		for _, item := range q.items {
			for _, condition := range conditions {
				if condition(item) {
					foundItem = item
					break
				}
			}
			if foundItem != nil {
				break
			}
		}
	}

	if foundItem != nil {
		if err := q.decoder.Decode(foundItem, e); err != nil {
			return fmt.Errorf("failed to decode entity: %w", err)
		}
		return nil
	}

	return ErrEntityNotFound
}

type FindCondition func(item map[string]types.AttributeValue) bool

func FieldValue(field string, value interface{}) FindCondition {
	return func(item map[string]types.AttributeValue) bool {
		if av, ok := item[field]; ok {
			switch v := av.(type) {
			case *types.AttributeValueMemberS:
				return v.Value == value
			case *types.AttributeValueMemberN:
				return v.Value == fmt.Sprintf("%v", value)
			case *types.AttributeValueMemberBOOL:
				return v.Value == value
			}
		}
		return false
	}
}

func (q *Query) Next(ctx context.Context) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.index++
	if q.index <= len(q.items) {
		return true
	}

	for !q.isLastPage() {
		err := q.nextQuery(ctx)
		if err != nil {
			return false
		}

		if q.index <= len(q.items) {
			return true
		}
	}

	return false
}

func (q *Query) nextQuery(ctx context.Context) error {
	q.input.ExclusiveStartKey = q.output.LastEvaluatedKey

	output, err := q.client.Query(ctx, q.input)
	if err != nil {
		q.error = err
		return err
	}

	q.output = output
	if len(q.output.Items) > 0 {
		q.items = append(q.items, q.output.Items...)
	}
	return nil
}

func (q *Query) Reset() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.index = 0
	q.error = nil
}

func (q *Query) Decode(e Entity) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.index <= 0 || q.index > len(q.items) {
		return ErrIndexOutOfRange
	}

	return q.decoder.Decode(q.items[q.index-1], e)
}

func (q *Query) Error() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.error
}
