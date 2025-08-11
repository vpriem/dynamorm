package dynamorm

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestORFilter(t *testing.T) {
	tests := []struct {
		name           string
		filter         Filter
		expectedExpr   string
		expectedValues map[string]types.AttributeValue
		expectedNames  map[string]string
	}{
		{
			name:         "OR with filter on same field",
			filter:       OR(EQ("Name", "John"), EQ("Name", "Jane")),
			expectedExpr: "(#Name = :Name OR #Name = :Name_1)",
			expectedValues: map[string]types.AttributeValue{
				":Name":   &types.AttributeValueMemberS{Value: "John"},
				":Name_1": &types.AttributeValueMemberS{Value: "Jane"},
			},
			expectedNames: map[string]string{
				"#Name": "Name",
			},
		},
		{
			name:         "AND with filter on same field",
			filter:       AND(EQ("Name", "John"), EQ("Name", "Jane")),
			expectedExpr: "(#Name = :Name AND #Name = :Name_1)",
			expectedValues: map[string]types.AttributeValue{
				":Name":   &types.AttributeValueMemberS{Value: "John"},
				":Name_1": &types.AttributeValueMemberS{Value: "Jane"},
			},
			expectedNames: map[string]string{
				"#Name": "Name",
			},
		},
		{
			name: "AND OR combined on same field",
			filter: AND(
				OR(EQ("Name", "John"), EQ("Name", "Jane")),
				OR(EQ("Name", "James"), EQ("Name", "Jacky")),
			),
			expectedExpr: "((#Name = :Name OR #Name = :Name_1) AND (#Name = :Name_2 OR #Name = :Name_3))",
			expectedValues: map[string]types.AttributeValue{
				":Name":   &types.AttributeValueMemberS{Value: "John"},
				":Name_1": &types.AttributeValueMemberS{Value: "Jane"},
				":Name_2": &types.AttributeValueMemberS{Value: "James"},
				":Name_3": &types.AttributeValueMemberS{Value: "Jacky"},
			},
			expectedNames: map[string]string{
				"#Name": "Name",
			},
		},
		{
			name: "AND OR combined with multiples fields",
			filter: AND(
				OR(EQ("Name", "John"), GTE("Age", 30)),
				OR(EQ("Name", "Jane"), LT("Age", 31)),
			),
			expectedExpr: "((#Name = :Name OR #Age >= :Age) AND (#Name = :Name_1 OR #Age < :Age_1))",
			expectedValues: map[string]types.AttributeValue{
				":Name":   &types.AttributeValueMemberS{Value: "John"},
				":Name_1": &types.AttributeValueMemberS{Value: "Jane"},
				":Age":    &types.AttributeValueMemberN{Value: "30"},
				":Age_1":  &types.AttributeValueMemberN{Value: "31"},
			},
			expectedNames: map[string]string{
				"#Name": "Name",
				"#Age":  "Age",
			},
		},
		{
			name: "AND combined with existing filter",
			filter: func(input *Input) {
				EQ("Name", "John")(input)
				OR(GT("Age", 30), LT("Age", 40))(input)
			},
			expectedExpr: "#Name = :Name AND (#Age > :Age OR #Age < :Age_1)",
			expectedValues: map[string]types.AttributeValue{
				":Name":  &types.AttributeValueMemberS{Value: "John"},
				":Age":   &types.AttributeValueMemberN{Value: "30"},
				":Age_1": &types.AttributeValueMemberN{Value: "40"},
			},
			expectedNames: map[string]string{
				"#Name": "Name",
				"#Age":  "Age",
			},
		},
		{
			name:         "Nested AND with single filter",
			filter:       AND(AND(AND(EQ("Name", "John")))),
			expectedExpr: "#Name = :Name",
			expectedValues: map[string]types.AttributeValue{
				":Name": &types.AttributeValueMemberS{Value: "John"},
			},
			expectedNames: map[string]string{
				"#Name": "Name",
			},
		},
		{
			name:         "NOT with two filters",
			filter:       NOT(EQ("Name", "John"), LT("Age", 40)),
			expectedExpr: "(#Name = :Name NOT #Age < :Age)",
			expectedValues: map[string]types.AttributeValue{
				":Name": &types.AttributeValueMemberS{Value: "John"},
				":Age":  &types.AttributeValueMemberN{Value: "40"},
			},
			expectedNames: map[string]string{
				"#Name": "Name",
				"#Age":  "Age",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &Input{}
			tt.filter(input)
			require.Equal(t, tt.expectedExpr, *input.FilterExpression)
			require.Equal(t, tt.expectedValues, input.ExpressionAttributeValues)
			require.Equal(t, tt.expectedNames, input.ExpressionAttributeNames)
		})
	}
}
