package dynamorm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelect(t *testing.T) {
	input := &Input{}
	Select("PK", "SK")(input)

	require.Equal(t, "#PK, #SK", *input.ProjectionExpression)
	require.Equal(t, map[string]string{
		"#PK": "PK",
		"#SK": "SK",
	}, input.ExpressionAttributeNames)
}

func TestSelectAppend(t *testing.T) {
	input := &Input{}
	Select("PK")(input)
	Select("SK")(input)

	require.Equal(t, "#PK, #SK", *input.ProjectionExpression)
	require.Equal(t, map[string]string{
		"#PK": "PK",
		"#SK": "SK",
	}, input.ExpressionAttributeNames)
}
