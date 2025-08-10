package dynamorm

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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

func (i *Input) refField(name string) string {
	if i.ExpressionAttributeNames == nil {
		i.ExpressionAttributeNames = map[string]string{}
	}

	ref := fmt.Sprintf("#%s", name)
	i.ExpressionAttributeNames[ref] = name
	return ref
}

func (i *Input) refFieldValue(name string, value interface{}) string {
	ref := fmt.Sprintf(":%s", name)
	ref = uniqueRef(ref, i.ExpressionAttributeValues)

	if i.ExpressionAttributeValues == nil {
		i.ExpressionAttributeValues = map[string]types.AttributeValue{}
	}

	av, err := attributevalue.Marshal(value)
	if err != nil {
		i.ExpressionAttributeValues[ref] = &types.AttributeValueMemberS{Value: fmt.Sprintf("%v", value)}
	} else {
		i.ExpressionAttributeValues[ref] = av
	}

	return ref
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

func uniqueRef(ref string, values map[string]types.AttributeValue) string {
	if _, exists := values[ref]; !exists {
		return ref
	}

	counter := 1

	prefixToFind := ref + "_"
	for k := range values {
		if strings.HasPrefix(k, prefixToFind) {
			counter++
		}
	}

	for {
		newRef := fmt.Sprintf("%s_%d", ref, counter)
		if _, exists := values[newRef]; !exists {
			return newRef
		}
		counter++
	}
}
