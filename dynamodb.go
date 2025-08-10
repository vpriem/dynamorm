package dynamorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

//go:generate mockgen -package=dynamorm_test -destination=dynamodb_mock_test.go . DynamoDB

// DynamoDB defines the interface for interacting with AWS DynamoDB service.
// This interface abstracts the AWS SDK's DynamoDB client, allowing for easier testing
// and flexibility in implementation.
type DynamoDB interface {
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

// WithBaseEndpoint returns a function that configures the DynamoDB client with a custom base endpoint.
// This is particularly useful for local test integration, allowing you to point the client
// to a local DynamoDB instance or alternative endpoint.
func WithBaseEndpoint(endpoint string) func(*dynamodb.Options) {
	return func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	}
}
