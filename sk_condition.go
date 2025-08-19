package dynamorm

import "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

// SkCondition represents a function that, given a sort key name, returns a
// KeyConditionBuilder to be used in a DynamoDB Query key condition. It is used
// by Storage.Query, Storage.QueryGSI1, and Storage.QueryGSI2 to build the SK
// part of the key condition expression.
type SkCondition func(key string) expression.KeyConditionBuilder

// SkEQ creates a sort key equality condition: SK = value.
func SkEQ(v interface{}) SkCondition {
	return func(key string) expression.KeyConditionBuilder {
		return expression.Key(key).Equal(expression.Value(v))
	}
}

// SkLTE creates a sort key "less than or equal" condition: SK <= value.
func SkLTE(v interface{}) SkCondition {
	return func(key string) expression.KeyConditionBuilder {
		return expression.Key(key).LessThanEqual(expression.Value(v))
	}
}

// SkLT creates a sort key "less than" condition: SK < value.
func SkLT(v interface{}) SkCondition {
	return func(key string) expression.KeyConditionBuilder {
		return expression.Key(key).LessThan(expression.Value(v))
	}
}

// SkGTE creates a sort key "greater than or equal" condition: SK >= value.
func SkGTE(v interface{}) SkCondition {
	return func(key string) expression.KeyConditionBuilder {
		return expression.Key(key).GreaterThanEqual(expression.Value(v))
	}
}

// SkGT creates a sort key "greater than" condition: SK > value.
func SkGT(v interface{}) SkCondition {
	return func(key string) expression.KeyConditionBuilder {
		return expression.Key(key).GreaterThan(expression.Value(v))
	}
}

// SkBeginsWith creates a sort key prefix condition: begins_with(SK, prefix).
func SkBeginsWith(prefix string) SkCondition {
	return func(key string) expression.KeyConditionBuilder {
		return expression.Key(key).BeginsWith(prefix)
	}
}

// SkBetween creates a sort key range condition: SK BETWEEN lower AND upper.
func SkBetween(lower, upper interface{}) SkCondition {
	return func(key string) expression.KeyConditionBuilder {
		return expression.Key(key).Between(
			expression.Value(lower),
			expression.Value(upper),
		)
	}
}
