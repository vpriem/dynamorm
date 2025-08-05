package dynamorm

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Condition is a function that modifies a DynamoDB QueryInput by adding a key condition expression.
// Conditions can be used with storage.Query, storage.QueryGSI1, and storage.QueryGSI2 methods
// to add KeyConditionExpression to the query SK, allowing for more refined query results.
type Condition func(string, *dynamodb.QueryInput)

// SkEQ creates a Condition that adds an equality (=) key condition expression to the query SK
var SkEQ = newCondition("%s = %s")

// SkNEQ creates a Condition that adds a not-equal (<>) key condition expression to the query SK
var SkNEQ = newCondition("%s <> %s")

// SkLT creates a Condition that adds a less-than (<) key condition expression to the query SK
var SkLT = newCondition("%s < %s")

// SkLTE creates a Condition that adds a less-than-or-equal (<=) key condition expression to the query SK
var SkLTE = newCondition("%s <= %s")

// SkGT creates a Condition that adds a greater-than (>) key condition expression to the query SK
var SkGT = newCondition("%s > %s")

// SkGTE creates a Condition that adds a greater-than-or-equal (>=) key condition expression to the query SK
var SkGTE = newCondition("%s >= %s")

// SkBeginsWith creates a Condition that adds a begins_with function key condition expression to the query SK
var SkBeginsWith = newCondition("begins_with(%s, %s)")

func newCondition(format string) func(interface{}) Condition {
	return func(value interface{}) Condition {
		return func(field string, input *dynamodb.QueryInput) {
			key := fmt.Sprintf(":%s", field)
			key = uniqueKey(key, input.ExpressionAttributeValues)

			var expr []string
			if input.KeyConditionExpression != nil {
				expr = append(expr, *input.KeyConditionExpression)
			}
			expr = append(expr, fmt.Sprintf(format, field, key))
			input.KeyConditionExpression = aws.String(strings.Join(expr, " AND "))

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

func uniqueKey(key string, expressionValues map[string]types.AttributeValue) string {
	if _, exists := expressionValues[key]; !exists {
		return key
	}

	counter := 1
	for {
		newKey := fmt.Sprintf("%s%d", key, counter)
		if _, exists := expressionValues[newKey]; !exists {
			return newKey
		}
		counter++
	}
}
