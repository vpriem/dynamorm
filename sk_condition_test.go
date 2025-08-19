package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestSkCondition(t *testing.T) {
	key := "SK"

	cases := []struct {
		name     string
		expected expression.KeyConditionBuilder
		actual   expression.KeyConditionBuilder
	}{
		{
			name:     "EQ string",
			expected: expression.Key(key).Equal(expression.Value("v")),
			actual:   dynamorm.SkEQ("v")(key),
		},
		{
			name:     "LTE int",
			expected: expression.Key(key).LessThanEqual(expression.Value(10)),
			actual:   dynamorm.SkLTE(10)(key),
		},
		{
			name:     "LT int",
			expected: expression.Key(key).LessThan(expression.Value(5)),
			actual:   dynamorm.SkLT(5)(key),
		},
		{
			name:     "GTE int",
			expected: expression.Key(key).GreaterThanEqual(expression.Value(20)),
			actual:   dynamorm.SkGTE(20)(key),
		},
		{
			name:     "GT int",
			expected: expression.Key(key).GreaterThan(expression.Value(3)),
			actual:   dynamorm.SkGT(3)(key),
		},
		{
			name:     "BeginsWith prefix",
			expected: expression.Key(key).BeginsWith("prefix#"),
			actual:   dynamorm.SkBeginsWith("prefix#")(key),
		},
		{
			name: "Between range",
			expected: expression.Key(key).Between(
				expression.Value(100),
				expression.Value(200),
			),
			actual: dynamorm.SkBetween(100, 200)(key),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.actual)
		})
	}
}
