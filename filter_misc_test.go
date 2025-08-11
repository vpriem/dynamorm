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

func TestScanIndexForward(t *testing.T) {
	input := &dynamorm.Input{}
	require.Nil(t, input.ScanIndexForward)

	dynamorm.ScanIndexForward(true)(input)
	require.NotNil(t, input.ScanIndexForward)
	require.Equal(t, true, *input.ScanIndexForward)

	dynamorm.ScanIndexForward(false)(input)
	require.NotNil(t, input.ScanIndexForward)
	require.Equal(t, false, *input.ScanIndexForward)
}

func TestConsistentRead(t *testing.T) {
	input := &dynamorm.Input{}
	require.Nil(t, input.ConsistentRead)

	dynamorm.ConsistentRead(true)(input)
	require.NotNil(t, input.ConsistentRead)
	require.Equal(t, true, *input.ConsistentRead)

	dynamorm.ConsistentRead(false)(input)
	require.NotNil(t, input.ConsistentRead)
	require.Equal(t, false, *input.ConsistentRead)
}
