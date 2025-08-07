package dynamorm_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestLimit(t *testing.T) {
	input := &dynamorm.Input{}
	require.Nil(t, input.Limit)

	dynamorm.Limit(10)(input)
	require.NotNil(t, input.Limit)
	require.Equal(t, int32(10), *input.Limit)
}
