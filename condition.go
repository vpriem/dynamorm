package dynamorm

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Condition is a function that modifies a query Input by adding a key condition expression.
// Conditions can be used with storage.Query, storage.QueryGSI1, and storage.QueryGSI2 methods
// to add KeyConditionExpression to the query SK, allowing for more refined query results.
type Condition func(string, *Input)

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
		return func(field string, input *Input) {
			valueRef := input.refFieldValue(field, value)

			var expr []string
			if input.KeyConditionExpression != nil && *input.KeyConditionExpression != "" {
				expr = append(expr, *input.KeyConditionExpression)
			}
			expr = append(expr, fmt.Sprintf(format, field, valueRef))

			input.KeyConditionExpression = aws.String(strings.Join(expr, " AND "))
		}
	}
}
