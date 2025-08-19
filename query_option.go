package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// QueryOption customizes the DynamoDB QueryInput built by Storage.Query, Storage.QueryGSI1,
// and Storage.QueryGSI2.
type QueryOption func(*dynamodb.QueryInput, BuilderInterface) BuilderInterface

// QueryFilter applies a filter expression to the Query operation.
// Use AWS SDK's expression builders to compose complex filters.
func QueryFilter(filter expression.ConditionBuilder) QueryOption {
	return func(_ *dynamodb.QueryInput, builder BuilderInterface) BuilderInterface {
		return builder.WithFilter(filter)
	}
}

// QueryAttribute limits the attributes returned by the Query operation by applying
// a projection on the expression builder.
func QueryAttribute(attrs ...string) QueryOption {
	return func(_ *dynamodb.QueryInput, builder BuilderInterface) BuilderInterface {
		if len(attrs) == 0 {
			return nil
		}

		var proj expression.ProjectionBuilder
		for _, attr := range attrs {
			proj = proj.AddNames(expression.Name(attr))
		}

		return builder.WithProjection(proj)
	}
}

// QueryLimit sets the maximum number of items DynamoDB should evaluate per Query request
// by assigning the Limit field on the QueryInput. This controls page size.
func QueryLimit(count int32) QueryOption {
	return func(input *dynamodb.QueryInput, _ BuilderInterface) BuilderInterface {
		input.Limit = aws.Int32(count)
		return nil
	}
}

// QueryForward controls the sort order of the results for a Query operation.
// When true, results are returned in ascending order by the sort key; when false,
// results are returned in descending order.
func QueryForward(forward bool) QueryOption {
	return func(input *dynamodb.QueryInput, _ BuilderInterface) BuilderInterface {
		input.ScanIndexForward = aws.Bool(forward)
		return nil
	}
}

// QueryConsistent controls whether the Query operation uses strongly consistent reads.
// When true, strongly consistent reads are used; when false, eventually consistent reads are used.
func QueryConsistent(consistent bool) QueryOption {
	return func(input *dynamodb.QueryInput, _ BuilderInterface) BuilderInterface {
		input.ConsistentRead = aws.Bool(consistent)
		return nil
	}
}
