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
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// StorageInterface defines the operations for interacting with DynamoDB.
// It provides methods for retrieving, querying, saving, and removing entities.
type StorageInterface interface {
	// Get retrieves an entity from DynamoDB by its partition key (PK) and sort key (SK).
	// It calls entity.PkSk() to determine how to query the entity.
	// Optional Get options can customize the underlying GetItemInput request.
	// Returns ErrEntityNotFound if the item doesn't exist in the table.
	Get(context.Context, Entity, ...GetOption) error

	// Query performs a query operation on the table using the partition key (PK).
	// An optional SK condition can be provided to refine the query, as well as additional filters.
	// It returns a QueryInterface for iterating through the results.
	Query(ctx context.Context, pkValue string, skCond Condition, filters ...Filter) (QueryInterface, error)

	// QueryGSI1 performs a query operation on the Global Secondary Index 1.
	// An optional SK condition can be provided to refine the query, as well as additional filters.
	// It returns a QueryInterface for iterating through the results.
	QueryGSI1(ctx context.Context, pkValue string, skCond Condition, filters ...Filter) (QueryInterface, error)

	// QueryGSI2 performs a query operation on the Global Secondary Index 2.
	// An optional SK condition can be provided to refine the query, as well as additional filters.
	// It returns a QueryInterface for iterating through the results.
	QueryGSI2(ctx context.Context, pkValue string, skCond Condition, filters ...Filter) (QueryInterface, error)

	// Scan performs a scan operation on the table.
	// Optional ScanOption(s) can customize the underlying ScanInput (e.g., limit, filter, projection).
	// It returns a QueryInterface for iterating through the results.
	Scan(context.Context, ...ScanOption) (QueryInterface, error)

	// ScanGSI1 performs a scan operation on the Global Secondary Index 1.
	// Optional ScanOption(s) can customize the underlying ScanInput (e.g., limit, filter, projection).
	// It returns a QueryInterface for iterating through the results.
	ScanGSI1(context.Context, ...ScanOption) (QueryInterface, error)

	// ScanGSI2 performs a scan operation on the Global Secondary Index 2.
	// Optional ScanOption(s) can customize the underlying ScanInput (e.g., limit, filter, projection).
	// It returns a QueryInterface for iterating through the results.
	ScanGSI2(context.Context, ...ScanOption) (QueryInterface, error)

	// Save persists a single entity to DynamoDB.
	// It calls entity.PkSk() to populate PK and SK, and entity.GSI1() and entity.GSI2()
	// to populate GSI PK and SK. The BeforeSave() hook is called on the entity before saving.
	// Optional SaveOption(s) can be provided, such as SaveCondition to apply a conditional write.
	Save(context.Context, Entity, ...SaveOption) error

	// BatchSave persists one or more entities to DynamoDB using BatchWriteItem.
	// It calls BeforeSave() and uses PkSk(), GSI1(), and GSI2() for each entity.
	// Note: DynamoDB limits BatchWriteItem to 25 items per request; larger inputs are chunked.
	BatchSave(context.Context, ...Entity) error

	// BatchRemove deletes one or more entities from DynamoDB using BatchWriteItem with DeleteRequests.
	// It uses PkSk() for each entity to compute the key.
	// Note: DynamoDB limits BatchWriteItem to 25 items per request; larger inputs are chunked.
	BatchRemove(context.Context, ...Entity) error

	// Update applies one or more update operations to an existing item identified by its PK/SK.
	// If the operation returns attributes, they are decoded into the provided entity; otherwise, the entity is left unchanged.
	Update(context.Context, Entity, expression.UpdateBuilder, ...UpdateOption) error

	// Remove deletes an entity from DynamoDB by its PK/SK.
	// It calls entity.PkSk() to determine how to delete the entity.
	// Returns an error if the operation fails.
	Remove(context.Context, Entity, ...RemoveOption) error

	// Transaction creates a new Transaction to batch multiple write operations
	// (put, update, delete) and execute them atomically.
	// The returned transaction uses the same table as the storage instance.
	Transaction() TransactionInterface
}

// Storage implements the StorageInterface for DynamoDB operations.
type Storage struct {
	table      string           // DynamoDB table name
	encoder    EncoderInterface // Encoder for marshaling Go structs to DynamoDB items
	decoder    DecoderInterface // Decoder for unmarshaling DynamoDB items to Go structs
	newBuilder CreateBuilder    // BuilderInterface factory
	client     DynamoDB         // DynamoDB client
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

	return &Storage{table, cfg.Encoder, cfg.Decoder, cfg.NewBuilder, client}
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

func (s *Storage) Save(ctx context.Context, e Entity, opts ...SaveOption) error {
	item, err := s.createItem(e)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	}

	builder := s.newBuilder()
	var nextBuilder BuilderInterface
	for _, apply := range opts {
		if apply != nil {
			if b := apply(input, builder); b != nil {
				nextBuilder = b
			}
		}
	}
	if nextBuilder != nil {
		expr, err := nextBuilder.Build()
		if err != nil {
			return err
		}
		input.ConditionExpression = expr.Condition()
		input.ExpressionAttributeNames = expr.Names()
		input.ExpressionAttributeValues = expr.Values()
	}

	_, err = s.client.PutItem(ctx, input)
	return err
}

func (s *Storage) BatchSave(ctx context.Context, entities ...Entity) error {
	if len(entities) == 0 {
		return nil
	}

	batches := make([]types.WriteRequest, 0, len(entities))

	for _, e := range entities {
		item, err := s.createItem(e)
		if err != nil {
			return err
		}

		batches = append(batches, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	return s.batchWrite(ctx, batches)
}

func (s *Storage) Get(ctx context.Context, e Entity, opts ...GetOption) error {
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

	builder := s.newBuilder()
	var nextBuilder BuilderInterface
	for _, apply := range opts {
		if apply != nil {
			if b := apply(input, builder); b != nil {
				nextBuilder = b
			}
		}
	}
	if nextBuilder != nil {
		expr, err := nextBuilder.Build()
		if err != nil {
			return err
		}
		input.ProjectionExpression = expr.Projection()
		input.ExpressionAttributeNames = expr.Names()
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
	input := &Input{
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

func (s *Storage) query(ctx context.Context, input *Input, filters ...Filter) (QueryInterface, error) {
	for _, filter := range filters {
		filter(input)
	}

	in := input.ToQueryInput()
	out, err := s.client.Query(ctx, in)
	if err != nil {
		return nil, err
	}
	output := NewOutputFromQueryOutput(out)

	return NewQuery(s.client, input, nil, output, s.decoder), nil
}

func (s *Storage) QueryGSI1(ctx context.Context, pk string, condition Condition, filters ...Filter) (QueryInterface, error) {
	return s.queryGSI(ctx, "GSI1", pk, condition, filters...)
}

func (s *Storage) QueryGSI2(ctx context.Context, pk string, condition Condition, filters ...Filter) (QueryInterface, error) {
	return s.queryGSI(ctx, "GSI2", pk, condition, filters...)
}

func (s *Storage) queryGSI(ctx context.Context, index, pk string, condition Condition, filters ...Filter) (QueryInterface, error) {
	input := &Input{
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

func (s *Storage) Scan(ctx context.Context, opts ...ScanOption) (QueryInterface, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(s.table),
	}

	return s.scan(ctx, input, opts...)
}

func (s *Storage) ScanGSI1(ctx context.Context, opts ...ScanOption) (QueryInterface, error) {
	return s.scanGSI(ctx, "GSI1", opts...)
}

func (s *Storage) ScanGSI2(ctx context.Context, opts ...ScanOption) (QueryInterface, error) {
	return s.scanGSI(ctx, "GSI2", opts...)
}

func (s *Storage) scan(ctx context.Context, input *dynamodb.ScanInput, opts ...ScanOption) (QueryInterface, error) {
	builder := s.newBuilder()
	var nextBuilder BuilderInterface
	for _, apply := range opts {
		if apply != nil {
			if b := apply(input, builder); b != nil {
				nextBuilder = b
			}
		}
	}
	if nextBuilder != nil {
		expr, err := nextBuilder.Build()
		if err != nil {
			return nil, err
		}
		input.FilterExpression = expr.Filter()
		input.ProjectionExpression = expr.Projection()
		input.ExpressionAttributeNames = expr.Names()
		input.ExpressionAttributeValues = expr.Values()
	}

	out, err := s.client.Scan(ctx, input)
	if err != nil {
		return nil, err
	}
	output := NewOutputFromScanOutput(out)

	return NewQuery(s.client, nil, input, output, s.decoder), nil
}

func (s *Storage) scanGSI(ctx context.Context, index string, opts ...ScanOption) (QueryInterface, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(s.table),
		IndexName: aws.String(index),
	}

	return s.scan(ctx, input, opts...)
}

func (s *Storage) Update(ctx context.Context, e Entity, update expression.UpdateBuilder, opts ...UpdateOption) error {
	pk, sk := e.PkSk()
	if pk == "" {
		return errors.New("entity pk is empty")
	}
	if sk == "" {
		return errors.New("entity sk is empty")
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberS{Value: sk},
		},
	}

	builder := s.newBuilder().WithUpdate(update)
	for _, apply := range opts {
		if apply != nil {
			if b := apply(input, builder); b != nil {
				builder = b
			}
		}
	}
	expr, err := builder.Build()
	if err != nil {
		return err
	}
	input.UpdateExpression = expr.Update()
	input.ConditionExpression = expr.Condition()
	input.ExpressionAttributeNames = expr.Names()
	input.ExpressionAttributeValues = expr.Values()

	out, err := s.client.UpdateItem(ctx, input)
	if err != nil {
		return err
	}

	if (input.ReturnValues == ALL_NEW || input.ReturnValues == UPDATED_NEW) && out.Attributes != nil {
		if err := s.decoder.Decode(out.Attributes, e); err != nil {
			return fmt.Errorf("failed to decode entity: %w", err)
		}
	}

	return nil
}

func (s *Storage) Remove(ctx context.Context, e Entity, opts ...RemoveOption) error {
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

	builder := s.newBuilder()
	var nextBuilder BuilderInterface
	for _, apply := range opts {
		if apply != nil {
			if b := apply(input, builder); b != nil {
				nextBuilder = b
			}
		}
	}
	if nextBuilder != nil {
		expr, err := nextBuilder.Build()
		if err != nil {
			return err
		}
		input.ConditionExpression = expr.Condition()
		input.ExpressionAttributeNames = expr.Names()
		input.ExpressionAttributeValues = expr.Values()
	}

	_, err := s.client.DeleteItem(ctx, input)
	return err
}

func (s *Storage) Transaction() TransactionInterface {
	return NewTransaction(s.table, s.client, s.encoder, s.newBuilder)
}

func (s *Storage) BatchRemove(ctx context.Context, entities ...Entity) error {
	if len(entities) == 0 {
		return nil
	}

	batches := make([]types.WriteRequest, 0, len(entities))

	for _, e := range entities {
		pk, sk := e.PkSk()
		if pk == "" {
			return errors.New("entity pk is empty")
		}
		if sk == "" {
			return errors.New("entity sk is empty")
		}

		batches = append(batches, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: pk},
					"SK": &types.AttributeValueMemberS{Value: sk},
				},
			},
		})
	}

	return s.batchWrite(ctx, batches)
}

func (s *Storage) batchWrite(ctx context.Context, requests []types.WriteRequest) error {
	if len(requests) == 0 {
		return nil
	}

	const batchSize = 25

	for i := 0; i < len(requests); i += batchSize {
		end := i + batchSize
		if end > len(requests) {
			end = len(requests)
		}

		batch := requests[i:end]
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
