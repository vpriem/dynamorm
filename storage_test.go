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

func TestStorageSave(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	enc := NewMockEncoderInterface(ctrl)
	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	storage := dynamorm.NewStorage("TestTable", dynamo,
		dynamorm.WithEncoder(enc),
		dynamorm.WithBuilder(newBuilder),
	)

	cond := expression.AttributeExists(expression.Name("Attr"))
	names := map[string]string{}
	values := map[string]types.AttributeValue{}

	t.Run("should ensure interface", func(t *testing.T) {
		var _ dynamorm.StorageInterface = storage
	})

	t.Run("should save entity none", func(t *testing.T) {
		err := storage.BatchSave(context.TODO())
		require.NoError(t, err)
	})

	t.Run("should save entity", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")
		e.EXPECT().GSI1().Return("", "")
		e.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{
			"Attr": &types.AttributeValueMemberS{Value: "value"},
		}, nil)

		dynamo.EXPECT().
			PutItem(context.TODO(), &dynamodb.PutItemInput{
				TableName: aws.String("TestTable"),
				Item: map[string]types.AttributeValue{
					"PK":   &types.AttributeValueMemberS{Value: "PK#1"},
					"SK":   &types.AttributeValueMemberS{Value: "SK#1"},
					"Attr": &types.AttributeValueMemberS{Value: "value"},
				},
			}).
			Return(&dynamodb.PutItemOutput{}, nil)

		err := storage.Save(context.TODO(), e)
		require.NoError(t, err)
	})

	t.Run("should save entity with GSIs", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")
		e.EXPECT().GSI1().Return("GSI1PK#1", "GSI1SK#1")
		e.EXPECT().GSI2().Return("GSI2PK#1", "GSI2SK#1")

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{
			"Attr": &types.AttributeValueMemberS{Value: "value"},
		}, nil)

		dynamo.EXPECT().
			PutItem(context.TODO(), &dynamodb.PutItemInput{
				TableName: aws.String("TestTable"),
				Item: map[string]types.AttributeValue{
					"PK":     &types.AttributeValueMemberS{Value: "PK#1"},
					"SK":     &types.AttributeValueMemberS{Value: "SK#1"},
					"GSI1PK": &types.AttributeValueMemberS{Value: "GSI1PK#1"},
					"GSI1SK": &types.AttributeValueMemberS{Value: "GSI1SK#1"},
					"GSI2PK": &types.AttributeValueMemberS{Value: "GSI2PK#1"},
					"GSI2SK": &types.AttributeValueMemberS{Value: "GSI2SK#1"},
					"Attr":   &types.AttributeValueMemberS{Value: "value"},
				},
			}).
			Return(&dynamodb.PutItemOutput{}, nil)

		err := storage.Save(context.TODO(), e)
		require.NoError(t, err)
	})

	t.Run("should save entity with condition", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")
		e.EXPECT().GSI1().Return("", "")
		e.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{}, nil)

		builder.EXPECT().WithCondition(cond).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)
		expr.EXPECT().Condition().Return(aws.String("condition"))

		dynamo.EXPECT().
			PutItem(context.TODO(), &dynamodb.PutItemInput{
				TableName: aws.String("TestTable"),
				Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "PK#1"},
					"SK": &types.AttributeValueMemberS{Value: "SK#1"},
				},
				ConditionExpression:       aws.String("condition"),
				ExpressionAttributeNames:  names,
				ExpressionAttributeValues: values,
			}).
			Return(&dynamodb.PutItemOutput{}, nil)

		err := storage.Save(context.TODO(), e, dynamorm.SaveCondition(cond))
		require.NoError(t, err)
	})

	t.Run("should return error if pk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("", "SK#1")

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("should return error if BeforeSave fails", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(assert.AnError)

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntityBeforeSave)
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")
		e.EXPECT().GSI1().Return("", "")
		e.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{}, nil)

		ex := &types.ConditionalCheckFailedException{Message: aws.String("check failed")}

		dynamo.EXPECT().
			PutItem(gomock.Any(), gomock.Any()).
			Return(nil, ex)

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrClient)

		var checkErr *types.ConditionalCheckFailedException
		require.ErrorAs(t, err, &checkErr)
		require.Equal(t, "check failed", *checkErr.Message)
	})

	t.Run("should return encode error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		enc.EXPECT().Encode(e).Return(nil, assert.AnError)

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntityEncode)
	})
}

func TestStorageBatchSave(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	enc := NewMockEncoderInterface(ctrl)
	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithEncoder(enc))

	t.Run("should batch save entities", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")
		e1.EXPECT().GSI1().Return("", "")
		e1.EXPECT().GSI2().Return("", "")

		e2 := NewMockEntity(ctrl)
		e2.EXPECT().BeforeSave().Return(nil)
		e2.EXPECT().PkSk().Return("PK#2", "SK#2")
		e2.EXPECT().GSI1().Return("GSI1PK#1", "GSI1SK#1")
		e2.EXPECT().GSI2().Return("GSI2PK#1", "GSI2SK#1")

		enc.EXPECT().Encode(e1).Return(map[string]types.AttributeValue{
			"Attr1": &types.AttributeValueMemberS{Value: "value1"},
		}, nil)
		enc.EXPECT().Encode(e2).Return(map[string]types.AttributeValue{
			"Attr2": &types.AttributeValueMemberS{Value: "value2"},
		}, nil)

		expectedBatch := []types.WriteRequest{
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"PK":    &types.AttributeValueMemberS{Value: "PK#1"},
						"SK":    &types.AttributeValueMemberS{Value: "SK#1"},
						"Attr1": &types.AttributeValueMemberS{Value: "value1"},
					},
				},
			},
			{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"PK":     &types.AttributeValueMemberS{Value: "PK#2"},
						"SK":     &types.AttributeValueMemberS{Value: "SK#2"},
						"GSI1PK": &types.AttributeValueMemberS{Value: "GSI1PK#1"},
						"GSI1SK": &types.AttributeValueMemberS{Value: "GSI1SK#1"},
						"GSI2PK": &types.AttributeValueMemberS{Value: "GSI2PK#1"},
						"GSI2SK": &types.AttributeValueMemberS{Value: "GSI2SK#1"},
						"Attr2":  &types.AttributeValueMemberS{Value: "value2"},
					},
				},
			},
		}

		dynamo.EXPECT().
			BatchWriteItem(gomock.Any(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"TestTable": expectedBatch,
				},
			}).
			Return(&dynamodb.BatchWriteItemOutput{}, nil)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.NoError(t, err)
	})

	t.Run("should return error with unprocessed items", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")
		e1.EXPECT().GSI1().Return("", "")
		e1.EXPECT().GSI2().Return("", "")

		e2 := NewMockEntity(ctrl)
		e2.EXPECT().BeforeSave().Return(nil)
		e2.EXPECT().PkSk().Return("PK#2", "SK#2")
		e2.EXPECT().GSI1().Return("", "")
		e2.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(e1).Return(map[string]types.AttributeValue{}, nil)
		enc.EXPECT().Encode(e2).Return(map[string]types.AttributeValue{}, nil)

		dynamo.EXPECT().
			BatchWriteItem(gomock.Any(), gomock.Any()).
			Return(&dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]types.WriteRequest{
					"TestTable": {
						{},
					},
				},
			}, nil)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrBatch)
	})

	t.Run("should return error if empty pk", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("", "SK#1")

		e2 := NewMockEntity(ctrl)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("should return error if empty sk", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("PK#1", "")

		e2 := NewMockEntity(ctrl)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("should return error if BeforeSave fails", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(assert.AnError)

		e2 := NewMockEntity(ctrl)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrEntityBeforeSave)
	})

	t.Run("should return error if encode fails", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")

		e2 := NewMockEntity(ctrl)

		enc.EXPECT().Encode(e1).Return(nil, assert.AnError)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrEntityEncode)
	})

	t.Run("should return client error ", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")
		e1.EXPECT().GSI1().Return("", "")
		e1.EXPECT().GSI2().Return("", "")

		e2 := NewMockEntity(ctrl)
		e2.EXPECT().BeforeSave().Return(nil)
		e2.EXPECT().PkSk().Return("PK#2", "SK#2")
		e2.EXPECT().GSI1().Return("", "")
		e2.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(gomock.Any()).Return(map[string]types.AttributeValue{}, nil).AnyTimes()

		dynamo.EXPECT().
			BatchWriteItem(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})
}

func TestStorageBatchRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	storage := dynamorm.NewStorage("TestTable", dynamo)

	t.Run("should remove none", func(t *testing.T) {
		err := storage.BatchRemove(context.TODO())
		require.NoError(t, err)
	})

	t.Run("should batch remove entities", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")

		e2 := NewMockEntity(ctrl)
		e2.EXPECT().PkSk().Return("PK#2", "SK#2")

		expectedBatch := []types.WriteRequest{
			{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "PK#1"},
						"SK": &types.AttributeValueMemberS{Value: "SK#1"},
					},
				},
			},
			{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "PK#2"},
						"SK": &types.AttributeValueMemberS{Value: "SK#2"},
					},
				},
			},
		}

		dynamo.EXPECT().
			BatchWriteItem(gomock.Any(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					"TestTable": expectedBatch,
				},
			}).
			Return(&dynamodb.BatchWriteItemOutput{}, nil)

		err := storage.BatchRemove(context.TODO(), e1, e2)
		require.NoError(t, err)
	})

	t.Run("should return error with unprocessed items", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")

		e2 := NewMockEntity(ctrl)
		e2.EXPECT().PkSk().Return("PK#2", "SK#2")

		dynamo.EXPECT().
			BatchWriteItem(gomock.Any(), gomock.Any()).
			Return(&dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]types.WriteRequest{
					"TestTable": {{}},
				},
			}, nil)

		err := storage.BatchRemove(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrBatch)
	})

	t.Run("should return error if empty pk", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().PkSk().Return("", "SK#1")

		e2 := NewMockEntity(ctrl)
		err := storage.BatchRemove(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("should return error if empty sk", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().PkSk().Return("PK#1", "")

		e2 := NewMockEntity(ctrl)
		err := storage.BatchRemove(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("should return client error", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")
		e2 := NewMockEntity(ctrl)
		e2.EXPECT().PkSk().Return("PK#2", "SK#2")

		dynamo.EXPECT().
			BatchWriteItem(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := storage.BatchRemove(context.TODO(), e1, e2)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})
}

func TestStorageGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	dec := NewMockDecoderInterface(ctrl)
	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithDecoder(dec), dynamorm.WithBuilder(newBuilder))

	names := map[string]string{}

	t.Run("should return entity", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		out := &dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{},
		}

		dynamo.EXPECT().
			GetItem(context.TODO(), &dynamodb.GetItemInput{
				TableName: aws.String("TestTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "PK#1"},
					"SK": &types.AttributeValueMemberS{Value: "SK#1"},
				},
			}).
			Return(out, nil)

		dec.EXPECT().Decode(out.Item, e).Return(nil)

		err := storage.Get(context.TODO(), e)
		require.NoError(t, err)
	})

	t.Run("should get with option", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#2", "SK#2")

		builder.EXPECT().WithProjection(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Projection().Return(aws.String("proj"))
		expr.EXPECT().Names().Return(names)

		out := &dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{},
		}

		expected := &dynamodb.GetItemInput{
			TableName: aws.String("TestTable"),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "PK#2"},
				"SK": &types.AttributeValueMemberS{Value: "SK#2"},
			},
			ConsistentRead:           aws.Bool(true),
			ProjectionExpression:     aws.String("proj"),
			ExpressionAttributeNames: names,
		}

		dynamo.EXPECT().
			GetItem(context.TODO(), expected).
			Return(out, nil)

		dec.EXPECT().Decode(out.Item, e).Return(nil)

		err := storage.Get(context.TODO(), e,
			dynamorm.GetConsistent(true),
			dynamorm.GetAttribute("Attr1", "Attr2"),
		)
		require.NoError(t, err)
	})

	t.Run("should return ErrEntityNotFound", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		dynamo.EXPECT().
			GetItem(gomock.Any(), gomock.Any()).
			Return(&dynamodb.GetItemOutput{Item: nil}, nil)

		err := storage.Get(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntityNotFound)
	})

	t.Run("should return error if pk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("", "SK#1")

		err := storage.Get(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Get(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		dynamo.EXPECT().
			GetItem(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := storage.Get(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})

	t.Run("should return decode error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		out := &dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{},
		}

		dynamo.EXPECT().
			GetItem(gomock.Any(), gomock.Any()).
			Return(out, nil)

		dec.EXPECT().Decode(out.Item, e).Return(assert.AnError)

		err := storage.Get(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntityDecode)
	})

	t.Run("should return builder error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		builder.EXPECT().WithProjection(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(nil, assert.AnError)

		err := storage.Get(context.TODO(), e, dynamorm.GetAttribute("Attr"))
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestStorageQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	storage := dynamorm.NewStorage("TestTable", dynamo)

	out := &dynamodb.QueryOutput{
		Count: 1,
	}

	t.Run("should return query", func(t *testing.T) {
		dynamo.EXPECT().
			Query(context.TODO(), &dynamodb.QueryInput{
				TableName:              aws.String("TestTable"),
				KeyConditionExpression: aws.String("#0 = :0"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "pk-value"},
				},
				ExpressionAttributeNames: map[string]string{
					"#0": "PK",
				},
			}).
			Return(out, nil)

		query, err := storage.Query(context.TODO(), "pk-value", nil)
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return query with options", func(t *testing.T) {
		dynamo.EXPECT().
			Query(context.TODO(), &dynamodb.QueryInput{
				TableName:              aws.String("TestTable"),
				KeyConditionExpression: aws.String("(#0 = :0) AND (#1 = :1)"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "pk-value"},
					":1": &types.AttributeValueMemberS{Value: "sk-value"},
				},
				ExpressionAttributeNames: map[string]string{
					"#0": "PK",
					"#1": "SK",
				},
				Limit: aws.Int32(1),
			}).
			Return(out, nil)

		query, err := storage.Query(context.TODO(), "pk-value",
			dynamorm.SkEQ("sk-value"),
			dynamorm.QueryLimit(1),
		)
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should query by GSI1", func(t *testing.T) {
		dynamo.EXPECT().
			Query(context.TODO(), &dynamodb.QueryInput{
				TableName:              aws.String("TestTable"),
				IndexName:              aws.String("GSI1"),
				KeyConditionExpression: aws.String("#0 = :0"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "gsi1-pk-value"},
				},
				ExpressionAttributeNames: map[string]string{
					"#0": "GSI1PK",
				},
			}).
			Return(out, nil)

		query, err := storage.QueryGSI1(context.TODO(), "gsi1-pk-value", nil)
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should query by GSI1 with options", func(t *testing.T) {
		dynamo.EXPECT().
			Query(context.TODO(), &dynamodb.QueryInput{
				TableName:              aws.String("TestTable"),
				IndexName:              aws.String("GSI1"),
				KeyConditionExpression: aws.String("(#0 = :0) AND (#1 = :1)"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "gsi1-pk-value"},
					":1": &types.AttributeValueMemberS{Value: "gsi1-sk-value"},
				},
				ExpressionAttributeNames: map[string]string{
					"#0": "GSI1PK",
					"#1": "GSI1SK",
				},
				Limit: aws.Int32(1),
			}).
			Return(out, nil)

		query, err := storage.QueryGSI1(context.TODO(), "gsi1-pk-value",
			dynamorm.SkEQ("gsi1-sk-value"),
			dynamorm.QueryLimit(1),
		)
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should query by GSI2", func(t *testing.T) {
		dynamo.EXPECT().
			Query(context.TODO(), &dynamodb.QueryInput{
				TableName:              aws.String("TestTable"),
				IndexName:              aws.String("GSI2"),
				KeyConditionExpression: aws.String("#0 = :0"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":0": &types.AttributeValueMemberS{Value: "gsi2-pk-value"},
				},
				ExpressionAttributeNames: map[string]string{
					"#0": "GSI2PK",
				},
			}).
			Return(out, nil)

		query, err := storage.QueryGSI2(context.TODO(), "gsi2-pk-value", nil)
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return client error", func(t *testing.T) {
		dynamo.EXPECT().
			Query(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		query, err := storage.Query(context.TODO(), "PK#1", nil)
		require.ErrorIs(t, err, dynamorm.ErrClient)
		require.Nil(t, query)
	})
}

func TestStorageScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithBuilder(newBuilder))

	out := &dynamodb.ScanOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
		},
	}

	t.Run("should scan table", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName: aws.String("TestTable"),
			}).
			Return(out, nil)

		query, err := storage.Scan(ctx)
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should scan table with filter", func(t *testing.T) {
		filter := expression.Name("Attr").Equal(expression.Value("value"))

		names := map[string]string{}
		values := map[string]types.AttributeValue{}

		builder.EXPECT().WithFilter(filter).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Filter().Return(aws.String("filter"))
		expr.EXPECT().Projection().Return(aws.String("proj"))
		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)

		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName:                 aws.String("TestTable"),
				FilterExpression:          aws.String("filter"),
				ProjectionExpression:      aws.String("proj"),
				ExpressionAttributeNames:  names,
				ExpressionAttributeValues: values,
			}).
			Return(out, nil)

		query, err := storage.Scan(ctx, dynamorm.ScanFilter(filter))
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return error", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		_, err := storage.Scan(ctx)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})
}

func TestStorageGSI1(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithBuilder(newBuilder))

	out := &dynamodb.ScanOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
		},
	}

	t.Run("should scan table", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName: aws.String("TestTable"),
				IndexName: aws.String("GSI1"),
			}).
			Return(out, nil)

		query, err := storage.ScanGSI1(ctx)
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should scan table with filter", func(t *testing.T) {
		filter := expression.Name("Attr").Equal(expression.Value("value"))

		names := map[string]string{}
		values := map[string]types.AttributeValue{}

		builder.EXPECT().WithFilter(filter).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Filter().Return(aws.String("filter"))
		expr.EXPECT().Projection().Return(aws.String("proj"))
		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)

		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName:                 aws.String("TestTable"),
				IndexName:                 aws.String("GSI1"),
				FilterExpression:          aws.String("filter"),
				ProjectionExpression:      aws.String("proj"),
				ExpressionAttributeNames:  names,
				ExpressionAttributeValues: values,
			}).
			Return(out, nil)

		query, err := storage.ScanGSI1(ctx, dynamorm.ScanFilter(filter))
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return error", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		_, err := storage.ScanGSI1(ctx)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})
}

func TestStorageGSI2(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithBuilder(newBuilder))

	out := &dynamodb.ScanOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
		},
	}

	t.Run("should scan table", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName: aws.String("TestTable"),
				IndexName: aws.String("GSI2"),
			}).
			Return(out, nil)

		query, err := storage.ScanGSI2(ctx)
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should scan table with filter", func(t *testing.T) {
		filter := expression.Name("Attr").Equal(expression.Value("value"))

		names := map[string]string{}
		values := map[string]types.AttributeValue{}

		builder.EXPECT().WithFilter(filter).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Filter().Return(aws.String("filter"))
		expr.EXPECT().Projection().Return(aws.String("proj"))
		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)

		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName:                 aws.String("TestTable"),
				IndexName:                 aws.String("GSI2"),
				FilterExpression:          aws.String("filter"),
				ProjectionExpression:      aws.String("proj"),
				ExpressionAttributeNames:  names,
				ExpressionAttributeValues: values,
			}).
			Return(out, nil)

		query, err := storage.ScanGSI2(ctx, dynamorm.ScanFilter(filter))
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return error", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		_, err := storage.ScanGSI2(ctx)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})
}

func TestStorageRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithBuilder(newBuilder))

	cond := expression.AttributeExists(expression.Name("Attr"))
	names := map[string]string{}
	values := map[string]types.AttributeValue{}

	t.Run("should remove entity", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		dynamo.EXPECT().
			DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
				TableName: aws.String("TestTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "PK#1"},
					"SK": &types.AttributeValueMemberS{Value: "SK#1"},
				},
			}).
			Return(&dynamodb.DeleteItemOutput{}, nil)

		err := storage.Remove(context.TODO(), e)
		require.NoError(t, err)
	})

	t.Run("should return error if pk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("", "SK#1")

		err := storage.Remove(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Remove(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		dynamo.EXPECT().
			DeleteItem(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := storage.Remove(context.TODO(), e)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})

	t.Run("should remove entity with condition", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		builder.EXPECT().WithCondition(cond).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)
		expr.EXPECT().Condition().Return(aws.String("condition"))

		dynamo.EXPECT().
			DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
				TableName: aws.String("TestTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "PK#1"},
					"SK": &types.AttributeValueMemberS{Value: "SK#1"},
				},
				ConditionExpression:       aws.String("condition"),
				ExpressionAttributeNames:  names,
				ExpressionAttributeValues: values,
			}).
			Return(&dynamodb.DeleteItemOutput{}, nil)

		err := storage.Remove(context.TODO(), e, dynamorm.RemoveCondition(cond))
		require.NoError(t, err)
	})

	t.Run("should return builder error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		builder.EXPECT().WithCondition(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(nil, assert.AnError)

		err := storage.Remove(context.TODO(), e, dynamorm.RemoveCondition(cond))
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestStorageUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	dec := NewMockDecoderInterface(ctrl)
	expr := NewMockExpression(ctrl)
	builder := NewMockBuilderInterface(ctrl)
	newBuilder := func() dynamorm.BuilderInterface { return builder }
	storage := dynamorm.NewStorage("TestTable", dynamo,
		dynamorm.WithDecoder(dec),
		dynamorm.WithBuilder(newBuilder))

	update := expression.Set(
		expression.Name("Attr"),
		expression.Value("Value"),
	)
	names := map[string]string{}
	values := map[string]types.AttributeValue{}

	t.Run("should update entity", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		builder.EXPECT().WithUpdate(update).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)
		expr.EXPECT().Update().Return(aws.String("update"))
		expr.EXPECT().Condition().Return(aws.String("condition"))

		dynamo.EXPECT().
			UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
				TableName: aws.String("TestTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "PK#1"},
					"SK": &types.AttributeValueMemberS{Value: "SK#1"},
				},
				UpdateExpression:          aws.String("update"),
				ConditionExpression:       aws.String("condition"),
				ExpressionAttributeNames:  names,
				ExpressionAttributeValues: values,
			}).
			Return(&dynamodb.UpdateItemOutput{}, nil)

		err := storage.Update(context.TODO(), e, update)
		require.NoError(t, err)
	})

	t.Run("should update entity and decode into", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		builder.EXPECT().WithUpdate(update).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)
		expr.EXPECT().Update().Return(aws.String("update"))
		expr.EXPECT().Condition().Return(aws.String("condition"))

		out := &dynamodb.UpdateItemOutput{
			Attributes: map[string]types.AttributeValue{
				"Name": &types.AttributeValueMemberS{Value: "John"},
				"Age":  &types.AttributeValueMemberN{Value: "30"},
			},
		}

		dec.EXPECT().Decode(out.Attributes, e).Return(nil)

		dynamo.EXPECT().
			UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
				TableName: aws.String("TestTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "PK#1"},
					"SK": &types.AttributeValueMemberS{Value: "SK#1"},
				},
				UpdateExpression:          aws.String("update"),
				ConditionExpression:       aws.String("condition"),
				ExpressionAttributeNames:  names,
				ExpressionAttributeValues: values,
				ReturnValues:              "ALL_NEW",
			}).
			Return(out, nil)

		err := storage.Update(context.TODO(), e, update,
			dynamorm.UpdateReturnValues(dynamorm.ALL_NEW),
		)
		require.NoError(t, err)
	})

	t.Run("should return error if pk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("", "SK#1")

		err := storage.Update(context.TODO(), e, update)
		require.ErrorIs(t, err, dynamorm.ErrEntityPkNotSet)
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Update(context.TODO(), e, update)
		require.ErrorIs(t, err, dynamorm.ErrEntitySkNotSet)
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		builder.EXPECT().WithUpdate(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(expr, nil)

		expr.EXPECT().Names().Return(names)
		expr.EXPECT().Values().Return(values)
		expr.EXPECT().Update().Return(aws.String("update"))
		expr.EXPECT().Condition().Return(aws.String("condition"))

		dynamo.EXPECT().
			UpdateItem(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := storage.Update(context.TODO(), e, update)
		require.ErrorIs(t, err, dynamorm.ErrClient)
	})

	t.Run("should return builder error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		builder.EXPECT().WithUpdate(gomock.Any()).Return(builder)
		builder.EXPECT().Build().Return(nil, assert.AnError)

		err := storage.Update(context.TODO(), e, update)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestStorageTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	storage := dynamorm.NewStorage("TestTable", dynamo)

	t.Run("should create transaction and execute save", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#TX", "SK#TX")
		e.EXPECT().GSI1().Return("", "")
		e.EXPECT().GSI2().Return("", "")

		dynamo.EXPECT().
			TransactWriteItems(gomock.Any(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String("TestTable"),
							Item: map[string]types.AttributeValue{
								"PK": &types.AttributeValueMemberS{Value: "PK#TX"},
								"SK": &types.AttributeValueMemberS{Value: "SK#TX"},
							},
						},
					},
				},
			}).
			Return(&dynamodb.TransactWriteItemsOutput{}, nil)

		tx := storage.Transaction()
		require.NoError(t, tx.AddSave(e))
		require.NoError(t, tx.Execute(context.TODO()))
	})
}
