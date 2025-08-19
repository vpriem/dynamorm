package dynamorm_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }

	t.Run("should ensure interface", func(t *testing.T) {
		var _ dynamorm.TransactionInterface = dynamorm.NewTransaction("Table", dynamo, nil, nil)
	})

	t.Run("should return nil when no items", func(t *testing.T) {
		tx := dynamorm.NewTransaction("Table", dynamo, nil, nil)
		require.NoError(t, tx.Execute(context.TODO()))
	})

	t.Run("should execute transaction with all operations", func(t *testing.T) {
		put := NewMockEntity(ctrl)
		put.EXPECT().BeforeSave().Return(nil)
		put.EXPECT().PkSk().Return("PK#PUT", "SK#PUT")
		put.EXPECT().GSI1().Return("GSI1PK#PUT", "GSI1SK#PUT")
		put.EXPECT().GSI2().Return("GSI2PK#PUT", "GSI2SK#PUT")

		enc := NewMockEncoderInterface(ctrl)
		enc.EXPECT().Encode(put).Return(map[string]types.AttributeValue{
			"Attr": &types.AttributeValueMemberS{Value: "value"},
		}, nil)

		up := NewMockEntity(ctrl)
		up.EXPECT().PkSk().Return("PK#UP", "SK#UP").AnyTimes()

		del := NewMockEntity(ctrl)
		del.EXPECT().PkSk().Return("PK#DEL", "SK#DEL")

		update := expression.Set(
			expression.Name("Attr"),
			expression.Value("Value"),
		)
		cond := expression.AttributeExists(expression.Name("Attr"))
		names := map[string]string{}
		values := map[string]types.AttributeValue{}

		builder.EXPECT().WithUpdate(update).Return(builder)
		builder.EXPECT().WithCondition(cond).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Names().Return(names).AnyTimes()
		expr.EXPECT().Values().Return(values).AnyTimes()
		expr.EXPECT().Update().Return(aws.String("update"))
		expr.EXPECT().Condition().Return(aws.String("condition")).AnyTimes()

		chk := NewMockEntity(ctrl)
		chk.EXPECT().PkSk().Return("PK#CHK", "SK#CHK")

		tx := dynamorm.NewTransaction("TestTable", dynamo, enc, newBuilder)
		require.NoError(t, tx.AddSave(put))
		require.NoError(t, tx.AddUpdate(up, update))
		require.NoError(t, tx.AddRemove(del))
		require.NoError(t, tx.AddConditionCheck(chk, cond))

		dynamo.EXPECT().
			TransactWriteItems(gomock.Any(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String("TestTable"),
							Item: map[string]types.AttributeValue{
								"PK":     &types.AttributeValueMemberS{Value: "PK#PUT"},
								"SK":     &types.AttributeValueMemberS{Value: "SK#PUT"},
								"GSI1PK": &types.AttributeValueMemberS{Value: "GSI1PK#PUT"},
								"GSI1SK": &types.AttributeValueMemberS{Value: "GSI1SK#PUT"},
								"GSI2PK": &types.AttributeValueMemberS{Value: "GSI2PK#PUT"},
								"GSI2SK": &types.AttributeValueMemberS{Value: "GSI2SK#PUT"},
								"Attr":   &types.AttributeValueMemberS{Value: "value"},
							},
						},
					},
					{
						Update: &types.Update{
							TableName: aws.String("TestTable"),
							Key: map[string]types.AttributeValue{
								"PK": &types.AttributeValueMemberS{Value: "PK#UP"},
								"SK": &types.AttributeValueMemberS{Value: "SK#UP"},
							},
							UpdateExpression:          aws.String("update"),
							ConditionExpression:       aws.String("condition"),
							ExpressionAttributeNames:  names,
							ExpressionAttributeValues: values,
						},
					},
					{
						Delete: &types.Delete{
							TableName: aws.String("TestTable"),
							Key: map[string]types.AttributeValue{
								"PK": &types.AttributeValueMemberS{Value: "PK#DEL"},
								"SK": &types.AttributeValueMemberS{Value: "SK#DEL"},
							},
						},
					},
					{
						ConditionCheck: &types.ConditionCheck{
							TableName: aws.String("TestTable"),
							Key: map[string]types.AttributeValue{
								"PK": &types.AttributeValueMemberS{Value: "PK#CHK"},
								"SK": &types.AttributeValueMemberS{Value: "SK#CHK"},
							},
							ConditionExpression:       aws.String("condition"),
							ExpressionAttributeNames:  names,
							ExpressionAttributeValues: values,
						},
					},
				},
			}).
			Return(&dynamodb.TransactWriteItemsOutput{}, nil)

		err := tx.Execute(context.TODO())
		require.NoError(t, err)
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		tx := dynamorm.NewTransaction("TestTable", dynamo, nil, newBuilder)
		require.NoError(t, tx.AddRemove(e))

		dynamo.EXPECT().
			TransactWriteItems(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := tx.Execute(context.TODO())
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})
}

func TestTransactionAddSave(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	enc := NewMockEncoderInterface(ctrl)
	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	tx := dynamorm.NewTransaction("TestTable", dynamo, enc, newBuilder)

	e := NewMockEntity(ctrl)
	cond := expression.AttributeExists(expression.Name("Attr"))

	t.Run("BeforeSave error", func(t *testing.T) {
		e.EXPECT().BeforeSave().Return(assert.AnError)

		err := tx.AddSave(e)
		require.ErrorIs(t, err, dynamorm.ErrEntityBeforeSave)
	})

	t.Run("empty pk", func(t *testing.T) {
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("", "SK")

		err := tx.AddSave(e)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("empty sk", func(t *testing.T) {
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK", "")

		err := tx.AddSave(e)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("encode error", func(t *testing.T) {
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK", "SK")

		enc.EXPECT().Encode(e).Return(nil, assert.AnError)

		err := tx.AddSave(e)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to encode entity")
	})

	t.Run("with condition", func(t *testing.T) {
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK", "SK")
		e.EXPECT().GSI1().Return("", "")
		e.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{}, nil)

		expr.EXPECT().Condition().Return(aws.String("condition"))
		expr.EXPECT().Names().Return(nil)
		expr.EXPECT().Values().Return(nil)

		builder.EXPECT().WithCondition(cond).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		err := tx.AddSave(e, dynamorm.SaveCondition(cond))
		require.NoError(t, err)
	})

	t.Run("builder error", func(t *testing.T) {
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK", "SK")
		e.EXPECT().GSI1().Return("", "")
		e.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{}, nil)

		builder.EXPECT().WithCondition(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(nil, assert.AnError)

		err := tx.AddSave(e, dynamorm.SaveCondition(cond))
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestTransactionAddUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	tx := dynamorm.NewTransaction("TestTable", dynamo, nil, newBuilder)

	e := NewMockEntity(ctrl)

	update := expression.Set(
		expression.Name("Attr"),
		expression.Value("Value"),
	)

	t.Run("empty pk", func(t *testing.T) {
		e.EXPECT().PkSk().Return("", "SK")

		err := tx.AddUpdate(e, update)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("empty sk", func(t *testing.T) {
		e.EXPECT().PkSk().Return("PK", "")

		err := tx.AddUpdate(e, update)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})
}

func TestTransactionAddRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	expr := NewMockExpression(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	tx := dynamorm.NewTransaction("TestTable", dynamo, nil, newBuilder)

	e := NewMockEntity(ctrl)
	cond := expression.AttributeExists(expression.Name("Attr"))

	t.Run("empty pk", func(t *testing.T) {
		e.EXPECT().PkSk().Return("", "SK")

		err := tx.AddRemove(e)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("empty sk", func(t *testing.T) {
		e.EXPECT().PkSk().Return("PK", "")

		err := tx.AddRemove(e)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("with condition", func(t *testing.T) {
		e.EXPECT().PkSk().Return("PK", "SK")

		expr.EXPECT().Condition().Return(aws.String("condition"))
		expr.EXPECT().Names().Return(nil)
		expr.EXPECT().Values().Return(nil)

		builder.EXPECT().WithCondition(cond).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		err := tx.AddRemove(e, dynamorm.RemoveCondition(cond))
		require.NoError(t, err)
	})

	t.Run("builder error", func(t *testing.T) {
		e.EXPECT().PkSk().Return("PK", "SK")

		builder.EXPECT().WithCondition(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(nil, assert.AnError)

		err := tx.AddRemove(e, dynamorm.RemoveCondition(cond))
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestTransactionAddConditionCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	tx := dynamorm.NewTransaction("TestTable", dynamo, nil, newBuilder)

	e := NewMockEntity(ctrl)
	cond := expression.AttributeExists(expression.Name("Attr"))

	t.Run("empty pk", func(t *testing.T) {
		e.EXPECT().PkSk().Return("", "SK")

		err := tx.AddConditionCheck(e, cond)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("empty sk", func(t *testing.T) {
		e.EXPECT().PkSk().Return("PK", "")

		err := tx.AddConditionCheck(e, cond)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("builder error", func(t *testing.T) {
		e.EXPECT().PkSk().Return("PK", "SK")

		builder.EXPECT().WithCondition(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(nil, assert.AnError)

		err := tx.AddConditionCheck(e, cond)
		require.Error(t, err)
		require.ErrorIs(t, err, assert.AnError)
	})
}
