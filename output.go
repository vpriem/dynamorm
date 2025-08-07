package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Output represents the result of a DynamoDB query or scan operation.
type Output struct {
	Count            int32
	ScannedCount     int32
	Items            []map[string]types.AttributeValue
	LastEvaluatedKey map[string]types.AttributeValue
}

// NewOutputFromQueryOutput converts an AWS SDK DynamoDB QueryOutput to the library's Output type.
func NewOutputFromQueryOutput(q *dynamodb.QueryOutput) *Output {
	return &Output{
		Count:            q.Count,
		ScannedCount:     q.ScannedCount,
		Items:            q.Items,
		LastEvaluatedKey: q.LastEvaluatedKey,
	}
}
