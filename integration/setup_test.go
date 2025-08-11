package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func setUp(t *testing.T) *dynamorm.Storage {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	require.NoError(t, err)
	dynamo := dynamodb.NewFromConfig(cfg, dynamorm.WithBaseEndpoint("http://localhost:8000"))

	tm := NewTableManager(dynamo, TableSchema)
	err = tm.CreateTableIfNotExists(context.TODO())
	require.NoError(t, err)

	return dynamorm.NewStorage("TestTable", dynamo)
}
