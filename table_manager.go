package dynamorm

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type TableManager struct {
	client  DynamoDB
	schema  *dynamodb.CreateTableInput
	waiter  *dynamodb.TableExistsWaiter
	timeout time.Duration
}

func NewTableManager(client DynamoDB, schema *dynamodb.CreateTableInput) *TableManager {
	return &TableManager{client, schema, dynamodb.NewTableExistsWaiter(client), 5 * time.Minute}
}

func (tm *TableManager) CreateTableIfNotExists(ctx context.Context) error {
	_, err := tm.client.CreateTable(ctx, tm.schema)
	if err != nil {
		var resourceInUseErr *types.ResourceInUseException
		if errors.As(err, &resourceInUseErr) {
			return nil
		}
		return fmt.Errorf("failed to create table: %w", err)
	}

	err = tm.waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: tm.schema.TableName,
	}, tm.timeout)
	if err != nil {
		return fmt.Errorf("failed to wait for table creation: %w", err)
	}

	return nil
}
