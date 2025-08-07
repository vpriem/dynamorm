package dynamorm

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestCondition(t *testing.T) {
	tests := []struct {
		name           string
		condition      Condition
		field          string
		expectedExpr   string
		expectedValues map[string]types.AttributeValue
	}{
		{
			name:         "EQ condition",
			condition:    SkEQ("value"),
			field:        "SK",
			expectedExpr: "SK = :SK",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberS{Value: "value"},
			},
		},
		{
			name:         "NEQ condition",
			condition:    SkNEQ("value"),
			field:        "SK",
			expectedExpr: "SK <> :SK",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberS{Value: "value"},
			},
		},
		{
			name:         "LT condition",
			condition:    SkLT(10),
			field:        "SK",
			expectedExpr: "SK < :SK",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberN{Value: "10"},
			},
		},
		{
			name:         "LTE condition",
			condition:    SkLTE(20),
			field:        "SK",
			expectedExpr: "SK <= :SK",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberN{Value: "20"},
			},
		},
		{
			name:         "GT condition",
			condition:    SkGT(30),
			field:        "SK",
			expectedExpr: "SK > :SK",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberN{Value: "30"},
			},
		},
		{
			name:         "GTE condition",
			condition:    SkGTE(40),
			field:        "SK",
			expectedExpr: "SK >= :SK",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberN{Value: "40"},
			},
		},
		{
			name:         "BeginsWith condition",
			condition:    SkBeginsWith("prefix"),
			field:        "SK",
			expectedExpr: "begins_with(SK, :SK)",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberS{Value: "prefix"},
			},
		},
		{
			name:         "Boolean condition",
			condition:    SkEQ(true),
			field:        "IsActive",
			expectedExpr: "IsActive = :IsActive",
			expectedValues: map[string]types.AttributeValue{
				":IsActive": &types.AttributeValueMemberBOOL{Value: true},
			},
		},
		{
			name: "Multiple conditions with same field",
			condition: func(field string, input *Input) {
				SkEQ("value0")(field, input)
				SkEQ("value1")(field, input)
			},
			field:        "SK",
			expectedExpr: "SK = :SK AND SK = :SK_1",
			expectedValues: map[string]types.AttributeValue{
				":SK":   &types.AttributeValueMemberS{Value: "value0"},
				":SK_1": &types.AttributeValueMemberS{Value: "value1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &Input{}
			tt.condition(tt.field, input)
			require.Equal(t, aws.String(tt.expectedExpr), input.KeyConditionExpression)
			require.Equal(t, tt.expectedValues, input.ExpressionAttributeValues)
		})
	}
}
