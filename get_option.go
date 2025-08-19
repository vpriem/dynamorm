package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// GetOption customizes the DynamoDB GetItemInput request built by Storage.Get.
type GetOption func(*dynamodb.GetItemInput, BuilderInterface) BuilderInterface

// GetConsistent sets the ConsistentRead flag on the GetItemInput request.
// When true, the read is strongly consistent; when false (default), it is eventually consistent.
func GetConsistent(consistent bool) GetOption {
	return func(input *dynamodb.GetItemInput, _ BuilderInterface) BuilderInterface {
		input.ConsistentRead = aws.Bool(consistent)
		return nil
	}
}

// GetAttribute limits the attributes returned by the GetItemInput request using
// a ProjectionExpression built from the provided attribute names
func GetAttribute(attrs ...string) GetOption {
	return func(input *dynamodb.GetItemInput, builder BuilderInterface) BuilderInterface {
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
