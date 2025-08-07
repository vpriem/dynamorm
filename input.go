package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Input represents the parameters for a DynamoDB query operation.
// It provides a simplified interface for the AWS SDK's QueryInput.
type Input struct {
	TableName                 *string
	ExclusiveStartKey         map[string]types.AttributeValue
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	FilterExpression          *string
	IndexName                 *string
	KeyConditionExpression    *string
	Limit                     *int32
}

// ToQueryInput converts the library's Input type to an AWS SDK DynamoDB QueryInput.
func (i *Input) ToQueryInput() *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		TableName:                 i.TableName,
		ExclusiveStartKey:         i.ExclusiveStartKey,
		ExpressionAttributeNames:  i.ExpressionAttributeNames,
		ExpressionAttributeValues: i.ExpressionAttributeValues,
		FilterExpression:          i.FilterExpression,
		IndexName:                 i.IndexName,
		KeyConditionExpression:    i.KeyConditionExpression,
		Limit:                     i.Limit,
	}
}

// NewInputFromQueryInput converts an AWS SDK DynamoDB QueryInput to the library's Input type.
func NewInputFromQueryInput(q *dynamodb.QueryInput) *Input {
	if q == nil {
		return &Input{}
	}

	return &Input{
		TableName:                 q.TableName,
		ExclusiveStartKey:         q.ExclusiveStartKey,
		ExpressionAttributeNames:  q.ExpressionAttributeNames,
		ExpressionAttributeValues: q.ExpressionAttributeValues,
		FilterExpression:          q.FilterExpression,
		IndexName:                 q.IndexName,
		KeyConditionExpression:    q.KeyConditionExpression,
		Limit:                     q.Limit,
	}
}
