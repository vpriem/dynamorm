package dynamorm

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		name           string
		filter         Filter
		expectedExpr   string
		expectedValues map[string]types.AttributeValue
	}{
		{
			name:         "EQ condition",
			filter:       EQ("Name", "John"),
			expectedExpr: "Name = :Name",
			expectedValues: map[string]types.AttributeValue{
				":Name": &types.AttributeValueMemberS{Value: "John"},
			},
		},
		{
			name:         "EQ condition bool",
			filter:       EQ("IsActive", true),
			expectedExpr: "IsActive = :IsActive",
			expectedValues: map[string]types.AttributeValue{
				":IsActive": &types.AttributeValueMemberBOOL{Value: true},
			},
		},
		{
			name:         "NEQ condition",
			filter:       NEQ("Name", "John"),
			expectedExpr: "Name <> :Name",
			expectedValues: map[string]types.AttributeValue{
				":Name": &types.AttributeValueMemberS{Value: "John"},
			},
		},
		{
			name:         "LT condition",
			filter:       LT("Age", 30),
			expectedExpr: "Age < :Age",
			expectedValues: map[string]types.AttributeValue{
				":Age": &types.AttributeValueMemberN{Value: "30"},
			},
		},
		{
			name:         "LTE condition",
			filter:       LTE("Age", 30),
			expectedExpr: "Age <= :Age",
			expectedValues: map[string]types.AttributeValue{
				":Age": &types.AttributeValueMemberN{Value: "30"},
			},
		},
		{
			name:         "GT condition",
			filter:       GT("Age", 30),
			expectedExpr: "Age > :Age",
			expectedValues: map[string]types.AttributeValue{
				":Age": &types.AttributeValueMemberN{Value: "30"},
			},
		},
		{
			name:         "GTE condition",
			filter:       GTE("Age", 30),
			expectedExpr: "Age >= :Age",
			expectedValues: map[string]types.AttributeValue{
				":Age": &types.AttributeValueMemberN{Value: "30"},
			},
		},
		{
			name:         "BeginsWith condition",
			filter:       BeginsWith("SK", "USER#"),
			expectedExpr: "begins_with(SK, :SK)",
			expectedValues: map[string]types.AttributeValue{
				":SK": &types.AttributeValueMemberS{Value: "USER#"},
			},
		},
		{
			name: "Multiple conditions",
			filter: func(input *dynamodb.QueryInput) {
				EQ("Name", "John")(input)
				BeginsWith("SK", "PROFILE#")(input)
			},
			expectedExpr: "Name = :Name AND begins_with(SK, :SK)",
			expectedValues: map[string]types.AttributeValue{
				":Name": &types.AttributeValueMemberS{Value: "John"},
				":SK":   &types.AttributeValueMemberS{Value: "PROFILE#"},
			},
		},
		{
			name: "Unique keys for same field",
			filter: func(input *dynamodb.QueryInput) {
				EQ("PK", "value0")(input)
				EQ("PK", "value1")(input)
				EQ("PK", "value2")(input)
			},
			expectedExpr: "PK = :PK AND PK = :PK1 AND PK = :PK2",
			expectedValues: map[string]types.AttributeValue{
				":PK":  &types.AttributeValueMemberS{Value: "value0"},
				":PK1": &types.AttributeValueMemberS{Value: "value1"},
				":PK2": &types.AttributeValueMemberS{Value: "value2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &dynamodb.QueryInput{}
			tt.filter(input)
			require.Equal(t, aws.String(tt.expectedExpr), input.FilterExpression)
			require.Equal(t, tt.expectedValues, input.ExpressionAttributeValues)
		})
	}
}
