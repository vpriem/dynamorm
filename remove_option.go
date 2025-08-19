package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// RemoveOption represents a function that can mutate the DynamoDB DeleteItemInput
// and/or the expression.Builder used to build the delete statement. It returns
// the (possibly new) BuilderInterface to allow chaining when the builder is immutable.
type RemoveOption func(*dynamodb.DeleteItemInput, BuilderInterface) BuilderInterface

// RemoveCondition returns a RemoveOption that applies a condition to the removal
// via the expression builder. This results in a ConditionExpression being added
// to the DeleteItemInput request when the builder is built.
func RemoveCondition(condition expression.ConditionBuilder) RemoveOption {
	return func(_ *dynamodb.DeleteItemInput, builder BuilderInterface) BuilderInterface {
		return builder.WithCondition(condition)
	}
}
