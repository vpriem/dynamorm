// Package dynamorm is a lightweight, flexible Object-Relational Mapping (ORM) library
// for Amazon DynamoDB in Go. It provides a simple, type-safe way to interact with
// DynamoDB tables using Go structs.
//
// DynamORM offers features like:
//   - Simple, intuitive API for common DynamoDB operations
//   - Type-safe mapping between Go structs and DynamoDB items
//   - Support for primary keys and global secondary indexes
//   - Composite primary key support and integrity
//   - Flexible query and filtering capabilities
//   - Lifecycle hooks for entities
package dynamorm

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// StorageInterface defines the operations for interacting with DynamoDB.
// It provides methods for retrieving, querying, saving, and removing entities.
type StorageInterface interface {
	// Get retrieves an entity from DynamoDB by its partition key (PK) and sort key (SK).
	// Call entity.PkSk() to determine how to query the entity.
	Get(context.Context, Entity) error

	// Query performs a query operation on the table using the partition key (PK).
	// An optional SK condition can be provided as well as filters
	// It returns a QueryInterface for iterating through the results.
	Query(ctx context.Context, pkValue string, skCond Condition, filters ...Filter) (QueryInterface, error)

	// QueryGSI1 performs a query operation on a Global Secondary Index 1.
	// An optional SK condition can be provided as well as filters
	// It returns a QueryInterface for iterating through the results.
	QueryGSI1(ctx context.Context, pkValue string, skCond Condition, filters ...Filter) (QueryInterface, error)

	// QueryGSI2 performs a query operation on a Global Secondary Index 2.
	// An optional SK condition can be provided as well as filters
	// It returns a QueryInterface for iterating through the results.
	QueryGSI2(ctx context.Context, pkValue string, skCond Condition, filters ...Filter) (QueryInterface, error)

	// Save persists one or more entities to DynamoDB.
	// Call entity.PkSk() to populate PK and SK.
	// Call entity.GSI1() and entity.GSI2() to populate GSI PK and SK.
	// If multiple entities are provided, a batch operation is performed.
	// Calls BeforeSave() hook before saving.
	Save(context.Context, ...Entity) error

	// Remove deletes an entity from DynamoDB by its PK/SK.
	// Call entity.PkSk() to determine how to delete the entity.
	Remove(context.Context, Entity) error
}

// Storage implements the StorageInterface for DynamoDB operations.
type Storage struct {
	table   string           // DynamoDB table name
	encoder EncoderInterface // Encoder for marshaling Go structs to DynamoDB items
	decoder DecoderInterface // Decoder for unmarshaling DynamoDB items to Go structs
	client  DynamoDB         // DynamoDB client
}

// NewStorage creates a new Storage instance with the specified table name and DynamoDB client.
// Optional configuration can be provided through Option functions.
//
// Example:
//
//	storage := dynamorm.NewStorage("MyTable", dynamoDBClient)
//
// With custom options:
//
//	storage := dynamorm.NewStorage("MyTable", dynamoDBClient,
//	    dynamorm.WithEncoder(customEncoder),
//	    dynamorm.WithDecoder(customDecoder),
//	)
func NewStorage(table string, client DynamoDB, optFns ...Option) *Storage {
	cfg := DefaultOptions()
	for _, optFn := range optFns {
		optFn(cfg)
	}

	return &Storage{table, cfg.Encoder, cfg.Decoder, client}
}

func (s *Storage) createItem(e Entity) (map[string]types.AttributeValue, error) {
	if err := e.BeforeSave(); err != nil {
		return nil, fmt.Errorf("failed to prepare save: %w", err)
	}

	pk, sk := e.PkSk()
	if pk == "" {
		return nil, errors.New("entity pk is empty")
	}
	if sk == "" {
		return nil, errors.New("entity sk is empty")
	}

	item, err := s.encoder.Encode(e)
	if err != nil {
		return nil, fmt.Errorf("failed to encode entity: %w", err)
	}
	item["PK"] = &types.AttributeValueMemberS{Value: pk}
	item["SK"] = &types.AttributeValueMemberS{Value: sk}

	pk, sk = e.GSI1()
	if pk != "" {
		item["GSI1PK"] = &types.AttributeValueMemberS{Value: pk}
		if sk != "" {
			item["GSI1SK"] = &types.AttributeValueMemberS{Value: sk}
		}
	}

	pk, sk = e.GSI2()
	if pk != "" {
		item["GSI2PK"] = &types.AttributeValueMemberS{Value: pk}
		if sk != "" {
			item["GSI2SK"] = &types.AttributeValueMemberS{Value: sk}
		}
	}

	return item, nil
}

func (s *Storage) save(ctx context.Context, e Entity) error {
	item, err := s.createItem(e)
	if err != nil {
		return err
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})

	return err
}

func (s *Storage) saveBatch(ctx context.Context, entities []Entity) error {
	writeRequests := make([]types.WriteRequest, 0, len(entities))

	for _, e := range entities {
		item, err := s.createItem(e)
		if err != nil {
			return err
		}

		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	const batchSize = 25

	for i := 0; i < len(writeRequests); i += batchSize {
		end := i + batchSize
		if end > len(writeRequests) {
			end = len(writeRequests)
		}

		batch := writeRequests[i:end]

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				s.table: batch,
			},
		}

		output, err := s.client.BatchWriteItem(ctx, input)
		if err != nil {
			return err
		}

		if unprocessed, ok := output.UnprocessedItems[s.table]; ok && len(unprocessed) > 0 {
			return fmt.Errorf("failed to process all items in batch")
		}
	}

	return nil
}

func (s *Storage) Save(ctx context.Context, entities ...Entity) error {
	if len(entities) == 0 {
		return nil
	}

	if len(entities) == 1 {
		return s.save(ctx, entities[0])
	}

	return s.saveBatch(ctx, entities)
}

func (s *Storage) Get(ctx context.Context, e Entity) error {
	pk, sk := e.PkSk()
	if pk == "" {
		return errors.New("entity pk is empty")
	}
	if sk == "" {
		return errors.New("entity sk is empty")
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberS{Value: sk},
		},
	}

	output, err := s.client.GetItem(ctx, input)
	if err != nil {
		return err
	}

	if output.Item == nil {
		return ErrEntityNotFound
	}

	if err = s.decoder.Decode(output.Item, e); err != nil {
		return fmt.Errorf("failed to decode entity: %w", err)
	}

	return nil
}

func (s *Storage) Query(ctx context.Context, pk string, condition Condition, filters ...Filter) (QueryInterface, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("PK = :PK"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":PK": &types.AttributeValueMemberS{Value: pk},
		},
	}

	if condition != nil {
		condition("SK", input)
	}

	return s.query(ctx, input, filters...)
}

func (s *Storage) query(ctx context.Context, input *dynamodb.QueryInput, filters ...Filter) (QueryInterface, error) {
	for _, filter := range filters {
		filter(input)
	}

	output, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}

	return NewQuery(s.client, input, output, s.decoder), nil
}

func (s *Storage) QueryGSI1(ctx context.Context, pk string, condition Condition, filters ...Filter) (QueryInterface, error) {
	return s.queryGSI(ctx, "GSI1", pk, condition, filters...)
}

func (s *Storage) QueryGSI2(ctx context.Context, pk string, condition Condition, filters ...Filter) (QueryInterface, error) {
	return s.queryGSI(ctx, "GSI2", pk, condition, filters...)
}

func (s *Storage) queryGSI(ctx context.Context, index, pk string, condition Condition, filters ...Filter) (QueryInterface, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(index),
		KeyConditionExpression: aws.String(fmt.Sprintf("%sPK = :%sPK", index, index)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			fmt.Sprintf(":%sPK", index): &types.AttributeValueMemberS{Value: pk},
		},
	}

	if condition != nil {
		condition(fmt.Sprintf("%sSK", index), input)
	}

	return s.query(ctx, input, filters...)
}

func (s *Storage) Remove(ctx context.Context, e Entity) error {
	pk, sk := e.PkSk()
	if pk == "" {
		return errors.New("entity pk is empty")
	}
	if sk == "" {
		return errors.New("entity sk is empty")
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberS{Value: sk},
		},
	}

	_, err := s.client.DeleteItem(ctx, input)
	return err
}
