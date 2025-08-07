package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestNewOutputFromQueryOutput(t *testing.T) {
	out := &dynamodb.QueryOutput{
		Count:        10,
		ScannedCount: 20,
		Items: []map[string]types.AttributeValue{
			{"id": &types.AttributeValueMemberS{Value: "item1"}},
			{"id": &types.AttributeValueMemberS{Value: "item2"}},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "lastKey"},
		},
	}

	output := dynamorm.NewOutputFromQueryOutput(out)
	require.Equal(t, &dynamorm.Output{
		Count:        10,
		ScannedCount: 20,
		Items: []map[string]types.AttributeValue{
			{"id": &types.AttributeValueMemberS{Value: "item1"}},
			{"id": &types.AttributeValueMemberS{Value: "item2"}},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "lastKey"},
		},
	}, output)
}
