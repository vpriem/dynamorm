package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// ScanOption customizes the DynamoDB ScanInput request built by Storage.Scan.
type ScanOption func(*dynamodb.ScanInput, BuilderInterface) BuilderInterface

// ScanLimit sets the maximum number of items to evaluate per Scan request by
// assigning the Limit field on the ScanInput.
func ScanLimit(count int32) ScanOption {
	return func(input *dynamodb.ScanInput, _ BuilderInterface) BuilderInterface {
		input.Limit = aws.Int32(count)
		return nil
	}
}

// ScanFilter applies a filter expression to the Scan operation.
func ScanFilter(filter expression.ConditionBuilder) ScanOption {
	return func(_ *dynamodb.ScanInput, builder BuilderInterface) BuilderInterface {
		return builder.WithFilter(filter)
	}
}

// ScanAttribute limits the attributes returned by the Scan operation.
func ScanAttribute(attrs ...string) ScanOption {
	return func(_ *dynamodb.ScanInput, builder BuilderInterface) BuilderInterface {
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
