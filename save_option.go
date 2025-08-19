package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// SaveOption represents a function that can mutate the DynamoDB PutItemInput
// and/or the expression.Builder used to build the put statement. It returns
// the (possibly new) BuilderInterface to allow chaining when the builder is immutable.
type SaveOption func(*dynamodb.PutItemInput, BuilderInterface) BuilderInterface

// SaveCondition returns a SaveOption that applies a condition to the save
// via the expression builder. This results in a ConditionExpression being added
// to the PutItem request when the builder is built.
func SaveCondition(condition expression.ConditionBuilder) SaveOption {
	return func(_ *dynamodb.PutItemInput, builder BuilderInterface) BuilderInterface {
		return builder.WithCondition(condition)
	}
}
