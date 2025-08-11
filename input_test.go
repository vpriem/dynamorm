package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestToQueryInput(t *testing.T) {
	in := &dynamodb.QueryInput{
		TableName:                 aws.String("table"),
		ConsistentRead:            aws.Bool(true),
		ExclusiveStartKey:         map[string]types.AttributeValue{"start": &types.AttributeValueMemberS{Value: "key"}},
		ExpressionAttributeNames:  map[string]string{"attr": "attrName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{"attr": &types.AttributeValueMemberS{Value: "value"}},
		FilterExpression:          aws.String("filter"),
		IndexName:                 aws.String("index"),
		KeyConditionExpression:    aws.String("condition"),
		Limit:                     aws.Int32(10),
		ProjectionExpression:      aws.String("proj"),
		ScanIndexForward:          aws.Bool(true),
	}

	input := &dynamorm.Input{
		TableName:                 aws.String("table"),
		ConsistentRead:            aws.Bool(true),
		ExclusiveStartKey:         map[string]types.AttributeValue{"start": &types.AttributeValueMemberS{Value: "key"}},
		ExpressionAttributeNames:  map[string]string{"attr": "attrName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{"attr": &types.AttributeValueMemberS{Value: "value"}},
		FilterExpression:          aws.String("filter"),
		IndexName:                 aws.String("index"),
		KeyConditionExpression:    aws.String("condition"),
		Limit:                     aws.Int32(10),
		ProjectionExpression:      aws.String("proj"),
		ScanIndexForward:          aws.Bool(true),
	}

	require.Equal(t, in, input.ToQueryInput())
	require.Nil(t, input.ToScanInput())
}

func TestToScanInput(t *testing.T) {
	in := &dynamodb.ScanInput{
		TableName:                 aws.String("table"),
		ConsistentRead:            aws.Bool(true),
		ExclusiveStartKey:         map[string]types.AttributeValue{"start": &types.AttributeValueMemberS{Value: "key"}},
		ExpressionAttributeNames:  map[string]string{"attr": "attrName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{"attr": &types.AttributeValueMemberS{Value: "value"}},
		FilterExpression:          aws.String("filter"),
		IndexName:                 aws.String("index"),
		Limit:                     aws.Int32(10),
		ProjectionExpression:      aws.String("proj"),
	}

	input := &dynamorm.Input{
		TableName:                 aws.String("table"),
		ConsistentRead:            aws.Bool(true),
		ExclusiveStartKey:         map[string]types.AttributeValue{"start": &types.AttributeValueMemberS{Value: "key"}},
		ExpressionAttributeNames:  map[string]string{"attr": "attrName"},
		ExpressionAttributeValues: map[string]types.AttributeValue{"attr": &types.AttributeValueMemberS{Value: "value"}},
		FilterExpression:          aws.String("filter"),
		IndexName:                 aws.String("index"),
		KeyConditionExpression:    aws.String("condition"),
		Limit:                     aws.Int32(10),
		IsScan:                    true,
		ProjectionExpression:      aws.String("proj"),
	}

	require.Equal(t, in, input.ToScanInput())
	require.Nil(t, input.ToQueryInput())
}
