package dynamorm

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestBETWEEN(t *testing.T) {
	input := &Input{}
	BETWEEN("Age", 18, 30)(input)

	require.Equal(t, "#Age BETWEEN :Age AND :Age_1", *input.FilterExpression)
	require.Equal(t, map[string]string{
		"#Age": "Age",
	}, input.ExpressionAttributeNames)
	require.Equal(t, map[string]types.AttributeValue{
		":Age":   &types.AttributeValueMemberN{Value: "18"},
		":Age_1": &types.AttributeValueMemberN{Value: "30"},
	}, input.ExpressionAttributeValues)
}
