package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Input represents the parameters for a DynamoDB query or scan operation.
// It provides a simplified interface for the AWS SDK's QueryInput and ScanInput.
type Input struct {
	TableName                 *string
	ExclusiveStartKey         map[string]types.AttributeValue
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	FilterExpression          *string
	IndexName                 *string
	KeyConditionExpression    *string
	Limit                     *int32
	IsScan                    bool
}

// ToQueryInput converts the library's Input type to an AWS SDK DynamoDB QueryInput.
func (i *Input) ToQueryInput() *dynamodb.QueryInput {
	if i.IsScan {
		return nil
	}

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

// ToScanInput converts the library's Input type to an AWS SDK DynamoDB ScanInput.
func (i *Input) ToScanInput() *dynamodb.ScanInput {
	if !i.IsScan {
		return nil
	}

	return &dynamodb.ScanInput{
		TableName:                 i.TableName,
		ExclusiveStartKey:         i.ExclusiveStartKey,
		ExpressionAttributeNames:  i.ExpressionAttributeNames,
		ExpressionAttributeValues: i.ExpressionAttributeValues,
		FilterExpression:          i.FilterExpression,
		IndexName:                 i.IndexName,
		Limit:                     i.Limit,
	}
}
