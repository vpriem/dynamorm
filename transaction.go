package dynamorm

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TransactionInterface describes a lightweight builder for DynamoDB transactional writes.
// It allows queuing multiple write operations (Put, Update, Delete) against a single table
// and executing them atomically via TransactWriteItems.
//
// Implementations should accumulate operations until Execute is called. If no operations
// were added, Execute should be a no-op and return nil.
type TransactionInterface interface {
	// AddSave adds a Put operation for the given entity to the transaction.
	AddSave(Entity, ...SaveOption) error
	// AddUpdate adds an Update operation for the given entity to the transaction.
	AddUpdate(Entity, expression.UpdateBuilder, ...UpdateOption) error
	// AddRemove adds a Delete operation for the given entity to the transaction.
	AddRemove(Entity, ...RemoveOption) error
	// AddConditionCheck adds a ConditionCheck operation for the given entity with the provided condition.
	AddConditionCheck(Entity, expression.ConditionBuilder) error
	// Execute executes the transaction.
	Execute(ctx context.Context) error
}

type Transaction struct {
	table      string
	client     DynamoDB
	encoder    EncoderInterface
	newBuilder CreateBuilder
	items      []types.TransactWriteItem
}

// NewTransaction creates a new Transaction with the provided DynamoDB client.
func NewTransaction(table string, client DynamoDB, encoder EncoderInterface, newBuilder CreateBuilder) *Transaction {
	if encoder == nil {
		encoder = DefaultEncoder()
	}
	if newBuilder == nil {
		newBuilder = NewBuilder
	}
	return &Transaction{
		table:      table,
		client:     client,
		encoder:    encoder,
		newBuilder: newBuilder,
	}
}

func (tx *Transaction) addItem(item types.TransactWriteItem) {
	tx.items = append(tx.items, item)
}

func (tx *Transaction) Execute(ctx context.Context) error {
	if len(tx.items) == 0 {
		return nil
	}

	_, err := tx.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: tx.items,
	})
	if err != nil {
		return NewClientError(err)
	}
	return nil
}

func (tx *Transaction) AddSave(e Entity, opts ...SaveOption) error {
	if err := e.BeforeSave(); err != nil {
		return fmt.Errorf("%w: %v", ErrEntityBeforeSave, err)
	}

	pk, sk := e.PkSk()
	if pk == "" {
		return ErrEntityPkNotSet
	}
	if sk == "" {
		return ErrEntitySkNotSet
	}

	item, err := tx.encoder.Encode(e)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrEntityEncode, err)
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

	input := &types.Put{
		TableName: aws.String(tx.table),
		Item:      item,
	}

	builder := tx.newBuilder()
	var nextBuilder BuilderInterface
	for _, apply := range opts {
		if apply != nil {
			if b := apply(&dynamodb.PutItemInput{}, builder); b != nil {
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

	tx.addItem(types.TransactWriteItem{Put: input})
	return nil
}

func (tx *Transaction) AddUpdate(e Entity, update expression.UpdateBuilder, opts ...UpdateOption) error {
	pk, sk := e.PkSk()
	if pk == "" {
		return ErrEntityPkNotSet
	}
	if sk == "" {
		return ErrEntitySkNotSet
	}

	builder := tx.newBuilder().WithUpdate(update)
	for _, apply := range opts {
		if apply != nil {
			if b := apply(&dynamodb.UpdateItemInput{}, builder); b != nil {
				builder = b
			}
		}
	}
	expr, err := builder.Build()
	if err != nil {
		return err
	}

	input := &types.Update{
		TableName: aws.String(tx.table),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberS{Value: sk},
		},
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	tx.addItem(types.TransactWriteItem{Update: input})
	return nil
}

func (tx *Transaction) AddRemove(e Entity, opts ...RemoveOption) error {
	pk, sk := e.PkSk()
	if pk == "" {
		return ErrEntityPkNotSet
	}
	if sk == "" {
		return ErrEntitySkNotSet
	}

	input := &types.Delete{
		TableName: aws.String(tx.table),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberS{Value: sk},
		},
	}

	builder := tx.newBuilder()
	var nextBuilder BuilderInterface
	for _, apply := range opts {
		if apply != nil {
			if b := apply(&dynamodb.DeleteItemInput{}, builder); b != nil {
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

	tx.addItem(types.TransactWriteItem{Delete: input})
	return nil
}

func (tx *Transaction) AddConditionCheck(e Entity, cond expression.ConditionBuilder) error {
	pk, sk := e.PkSk()
	if pk == "" {
		return ErrEntityPkNotSet
	}
	if sk == "" {
		return ErrEntitySkNotSet
	}

	builder := tx.newBuilder().WithCondition(cond)
	expr, err := builder.Build()
	if err != nil {
		return err
	}

	input := &types.ConditionCheck{
		TableName: aws.String(tx.table),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberS{Value: sk},
		},
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	tx.addItem(types.TransactWriteItem{ConditionCheck: input})
	return nil
}
