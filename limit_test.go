package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestLimit(t *testing.T) {
	input := &dynamodb.QueryInput{}
	require.Nil(t, input.Limit)
	dynamorm.Limit(10)(input)
	require.Equal(t, aws.Int32(10), input.Limit)
}
