package dynamorm

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// QueryInterface provides an interface to handle DynamoDB query results.
// It supports automatic pagination when iterating through results using Next().
type QueryInterface interface {
	// Count returns the number of items of the current the query result
	Count() int32
	// ScannedCount returns the number of items evaluated of the current the query result
	ScannedCount() int32
	// First retrieves the first matching entity from the query results
	// Optional FindCondition parameters can be provided to filter the results
	First(Entity, ...FindCondition) error
	// Next advances the iterator to the next item.
	// Returns true if there is a next item available, false otherwise.
	Next() bool
	// NextPage fetch the next page of results using the LastEvaluatedKey.
	// Returns true if there is more results to be fetched, false otherwise.
	NextPage(context.Context) (bool, error)
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
	input   *dynamodb.QueryInput
	output  *Output
	decoder DecoderInterface
	index   int
}

// NewQuery creates a new Query instance from the query input and output.
func NewQuery(client DynamoDB, input *dynamodb.QueryInput, output *Output, decoder DecoderInterface) *Query {
	if input == nil {
		input = &dynamodb.QueryInput{}
	}
	if output == nil {
		output = &Output{}
	}
	if decoder == nil {
		decoder = DefaultDecoder()
	}

	return &Query{
		client:  client,
		input:   input,
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

func (q *Query) First(e Entity, conditions ...FindCondition) error {
	var foundItem map[string]types.AttributeValue

	if len(conditions) == 0 {
		if len(q.output.Items) > 0 {
			foundItem = q.output.Items[0]
		}
	} else {
		for _, item := range q.output.Items {
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

func (q *Query) Next() bool {
	q.index++
	return q.index <= len(q.output.Items)
}

func (q *Query) NextPage(ctx context.Context) (bool, error) {
	if q.output.LastEvaluatedKey == nil {
		return false, nil
	}

	q.input.ExclusiveStartKey = q.output.LastEvaluatedKey

	out, err := q.client.Query(ctx, q.input)
	if err != nil {
		return false, err
	}

	q.output = NewOutputFromQueryOutput(out)
	q.index = 0
	return true, nil
}

func (q *Query) Reset() {
	q.index = 0
}

func (q *Query) Decode(e Entity) error {
	if q.index <= 0 || q.index > len(q.output.Items) {
		return ErrIndexOutOfRange
	}

	return q.decoder.Decode(q.output.Items[q.index-1], e)
}
