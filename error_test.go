package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestClientError(t *testing.T) {
	err := dynamorm.NewClientError(nil)
	require.ErrorIs(t, err, dynamorm.ErrClient)
	require.EqualError(t, err, dynamorm.ErrClient.Error())

	ex := &types.ThrottlingException{Message: aws.String("throttled")}
	err = dynamorm.NewClientError(ex)
	require.ErrorIs(t, err, dynamorm.ErrClient)
	require.EqualError(t, err, "client error: ThrottlingException: throttled")

	var checkErr *types.ThrottlingException
	require.ErrorAs(t, err, &checkErr)
	require.Equal(t, "throttled", *checkErr.Message)
}
