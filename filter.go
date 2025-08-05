package dynamorm

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Filter is a function that modifies a DynamoDB QueryInput by adding a filter expression.
// Filters can be used with storage.Query, storage.QueryGSI1, and storage.QueryGSI2 methods
// to add filter expressions to the query, allowing for more refined query results.
type Filter func(*dynamodb.QueryInput)

// EQ creates a Filter that adds an equality (=) filter expression to the query
var EQ = newFilter("%s = %s")

// NEQ creates a Filter that adds a not-equal (<>) filter expression to the query
var NEQ = newFilter("%s <> %s")

// LT creates a Filter that adds a less-than (<) filter expression to the query
var LT = newFilter("%s < %s")

// LTE creates a Filter that adds a less-than-or-equal (<=) filter expression to the query
var LTE = newFilter("%s <= %s")

// GT creates a Filter that adds a greater-than (>) filter expression to the query
var GT = newFilter("%s > %s")

// GTE creates a Filter that adds a greater-than-or-equal (>=) filter expression to the query
var GTE = newFilter("%s >= %s")

// BeginsWith creates a Filter that adds a begins_with function filter expression to the query
var BeginsWith = newFilter("begins_with(%s, %s)")

func newFilter(format string) func(string, interface{}) Filter {
	return func(field string, value interface{}) Filter {
		return func(input *dynamodb.QueryInput) {
			key := fmt.Sprintf(":%s", field)
			key = uniqueKey(key, input.ExpressionAttributeValues)

			name := fmt.Sprintf("#%s", field)
			if input.ExpressionAttributeNames == nil {
				input.ExpressionAttributeNames = map[string]string{}
			}
			input.ExpressionAttributeNames[name] = field

			var expr []string
			if input.FilterExpression != nil {
				expr = append(expr, *input.FilterExpression)
			}
			expr = append(expr, fmt.Sprintf(format, name, key))
			input.FilterExpression = aws.String(strings.Join(expr, " AND "))

			if input.ExpressionAttributeValues == nil {
				input.ExpressionAttributeValues = map[string]types.AttributeValue{}
			}

			switch v := value.(type) {
			case string:
				input.ExpressionAttributeValues[key] = &types.AttributeValueMemberS{Value: v}
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
				input.ExpressionAttributeValues[key] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", v)}
			case bool:
				input.ExpressionAttributeValues[key] = &types.AttributeValueMemberBOOL{Value: v}
			default:
				input.ExpressionAttributeValues[key] = &types.AttributeValueMemberS{Value: fmt.Sprintf("%v", v)}
			}
		}
	}
}
