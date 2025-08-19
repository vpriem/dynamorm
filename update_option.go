package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// UpdateOption represents a function that can mutate the DynamoDB UpdateItemInput
// and/or the expression.Builder used to build the update statement. It returns
// the (possibly new) BuilderInterface to allow chaining when the builder is immutable.
type UpdateOption func(*dynamodb.UpdateItemInput, BuilderInterface) BuilderInterface

// ReturnValue is an alias of types.ReturnValue for convenience when specifying
// the expected return values of an UpdateItem operation.
type ReturnValue = types.ReturnValue

// Common ReturnValue constants for UpdateItem.
const (
	NONE        types.ReturnValue = "NONE"
	ALL_OLD     types.ReturnValue = "ALL_OLD"
	UPDATED_OLD types.ReturnValue = "UPDATED_OLD"
	ALL_NEW     types.ReturnValue = "ALL_NEW"
	UPDATED_NEW types.ReturnValue = "UPDATED_NEW"
)

// UpdateReturnValues returns an UpdateOption that sets the ReturnValues field on
// the UpdateItemInput.
func UpdateReturnValues(v ReturnValue) UpdateOption {
	return func(input *dynamodb.UpdateItemInput, _ BuilderInterface) BuilderInterface {
		input.ReturnValues = v
		return nil
	}
}

// UpdateCondition returns an UpdateOption that applies a condition to the update
// via the expression builder. This results in a ConditionExpression being added
// to the UpdateItem request when the builder is built.
func UpdateCondition(condition expression.ConditionBuilder) UpdateOption {
	return func(_ *dynamodb.UpdateItemInput, builder BuilderInterface) BuilderInterface {
		return builder.WithCondition(condition)
	}
}
