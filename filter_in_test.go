package dynamorm

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestIN(t *testing.T) {
	input := &Input{}
	IN("Status", "PENDING", "ACTIVE", "FAILED")(input)

	require.Equal(t, "#Status IN (:Status, :Status_1, :Status_2)", *input.FilterExpression)
	require.Equal(t, map[string]string{
		"#Status": "Status",
	}, input.ExpressionAttributeNames)
	require.Equal(t, map[string]types.AttributeValue{
		":Status":   &types.AttributeValueMemberS{Value: "PENDING"},
		":Status_1": &types.AttributeValueMemberS{Value: "ACTIVE"},
		":Status_2": &types.AttributeValueMemberS{Value: "FAILED"},
	}, input.ExpressionAttributeValues)
}
