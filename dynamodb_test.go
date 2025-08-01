package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestWithBaseEndpoint(t *testing.T) {
	opts := &dynamodb.Options{}
	require.Nil(t, opts.BaseEndpoint)

	dynamorm.WithBaseEndpoint("http://localhost:8000")(opts)
	require.NotNil(t, opts.BaseEndpoint)
	require.Equal(t, "http://localhost:8000", *opts.BaseEndpoint)
}
