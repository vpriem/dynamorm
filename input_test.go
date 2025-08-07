package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestNewInputFromQueryInput(t *testing.T) {
	in := &dynamodb.QueryInput{
		TableName:                 aws.String("table"),
		ExclusiveStartKey:         map[string]types.AttributeValue{"start": &types.AttributeValueMemberS{Value: "key"}},
		ExpressionAttributeNames:  map[string]string{"attr": "attrName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{"attr": &types.AttributeValueMemberS{Value: "value"}},
		FilterExpression:          aws.String("filter"),
		IndexName:                 aws.String("index"),
		KeyConditionExpression:    aws.String("condition"),
		Limit:                     aws.Int32(10),
	}

	input := dynamorm.NewInputFromQueryInput(in)
	require.Equal(t, &dynamorm.Input{
		TableName:                 aws.String("table"),
		ExclusiveStartKey:         map[string]types.AttributeValue{"start": &types.AttributeValueMemberS{Value: "key"}},
		ExpressionAttributeNames:  map[string]string{"attr": "attrName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{"attr": &types.AttributeValueMemberS{Value: "value"}},
		FilterExpression:          aws.String("filter"),
		IndexName:                 aws.String("index"),
		KeyConditionExpression:    aws.String("condition"),
		Limit:                     aws.Int32(10),
	}, input)

	require.Equal(t, in, input.ToQueryInput())
}
