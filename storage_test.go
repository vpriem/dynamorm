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
	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithEncoder(enc))

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

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{}, nil)

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
				},
			}).
			Return(&dynamodb.PutItemOutput{}, nil)

		err := storage.Save(context.TODO(), e)
		require.NoError(t, err)
	})

	t.Run("should return error if pk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("", "SK#1")

		err := storage.Save(context.TODO(), e)
		require.EqualError(t, err, "entity pk is empty")
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Save(context.TODO(), e)
		require.EqualError(t, err, "entity sk is empty")
	})

	t.Run("should return error if BeforeSave fails", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(assert.AnError)

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")
		e.EXPECT().GSI1().Return("", "")
		e.EXPECT().GSI2().Return("", "")

		enc.EXPECT().Encode(e).Return(map[string]types.AttributeValue{}, nil)

		dynamo.EXPECT().
			PutItem(gomock.Any(), gomock.Any()).
			Return(&dynamodb.PutItemOutput{}, assert.AnError)

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("should return encode error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().BeforeSave().Return(nil)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		enc.EXPECT().Encode(e).Return(nil, assert.AnError)

		err := storage.Save(context.TODO(), e)
		require.ErrorIs(t, err, assert.AnError)
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to process all items in batch")
	})

	t.Run("should return error if empty pk", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("", "SK#1")

		e2 := NewMockEntity(ctrl)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.EqualError(t, err, "entity pk is empty")
	})

	t.Run("should return error if empty sk", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("PK#1", "")

		e2 := NewMockEntity(ctrl)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.EqualError(t, err, "entity sk is empty")
	})

	t.Run("should return error if BeforeSave fails", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(assert.AnError)

		e2 := NewMockEntity(ctrl)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("should return error if encode fails", func(t *testing.T) {
		e1 := NewMockEntity(ctrl)
		e1.EXPECT().BeforeSave().Return(nil)
		e1.EXPECT().PkSk().Return("PK#1", "SK#1")

		e2 := NewMockEntity(ctrl)

		enc.EXPECT().Encode(e1).Return(nil, assert.AnError)

		err := storage.BatchSave(context.TODO(), e1, e2)
		require.ErrorIs(t, err, assert.AnError)
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
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestStorageGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	dec := NewMockDecoderInterface(ctrl)
	storage := dynamorm.NewStorage("TestTable", dynamo, dynamorm.WithDecoder(dec))

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
		require.EqualError(t, err, "entity pk is empty")
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Get(context.TODO(), e)
		require.EqualError(t, err, "entity sk is empty")
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		dynamo.EXPECT().
			GetItem(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := storage.Get(context.TODO(), e)
		require.ErrorIs(t, err, assert.AnError)
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
				KeyConditionExpression: aws.String("PK = :PK AND begins_with(SK, :SK)"),
				FilterExpression:       aws.String("#IsActive = :IsActive"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":PK":       &types.AttributeValueMemberS{Value: "pk-value"},
					":SK":       &types.AttributeValueMemberS{Value: "sk-value"},
					":IsActive": &types.AttributeValueMemberBOOL{Value: true},
				},
				ExpressionAttributeNames: map[string]string{
					"#IsActive": "IsActive",
				},
			}).
			Return(out, nil)

		query, err := storage.Query(context.TODO(), "pk-value",
			dynamorm.SkBeginsWith("sk-value"),
			dynamorm.EQ("IsActive", true))
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should query by GSI1", func(t *testing.T) {
		dynamo.EXPECT().
			Query(context.TODO(), &dynamodb.QueryInput{
				TableName:              aws.String("TestTable"),
				IndexName:              aws.String("GSI1"),
				KeyConditionExpression: aws.String("GSI1PK = :GSI1PK AND GSI1SK = :GSI1SK"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":GSI1PK": &types.AttributeValueMemberS{Value: "gsi1-pk-value"},
					":GSI1SK": &types.AttributeValueMemberS{Value: "gsi1-sk-value"},
				},
			}).
			Return(out, nil)

		query, err := storage.QueryGSI1(context.TODO(), "gsi1-pk-value",
			dynamorm.SkEQ("gsi1-sk-value"))
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should query by GSI2", func(t *testing.T) {
		dynamo.EXPECT().
			Query(context.TODO(), &dynamodb.QueryInput{
				TableName:              aws.String("TestTable"),
				IndexName:              aws.String("GSI2"),
				KeyConditionExpression: aws.String("GSI2PK = :GSI2PK AND GSI2SK = :GSI2SK"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":GSI2PK": &types.AttributeValueMemberS{Value: "gsi2-pk-value"},
					":GSI2SK": &types.AttributeValueMemberS{Value: "gsi2-sk-value"},
				},
			}).
			Return(out, nil)

		query, err := storage.QueryGSI2(context.TODO(), "gsi2-pk-value",
			dynamorm.SkEQ("gsi2-sk-value"))
		require.NoError(t, err)
		require.NotNil(t, query)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return client error", func(t *testing.T) {
		dynamo.EXPECT().
			Query(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		query, err := storage.Query(context.TODO(), "PK#1", nil)
		require.ErrorIs(t, err, assert.AnError)
		require.Nil(t, query)
	})
}

func TestStorageScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	storage := dynamorm.NewStorage("TestTable", dynamo)

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
		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName:        aws.String("TestTable"),
				FilterExpression: aws.String("#Email = :Email"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"},
				},
				ExpressionAttributeNames: map[string]string{
					"#Email": "Email",
				},
			}).
			Return(out, nil)

		query, err := storage.Scan(ctx, dynamorm.EQ("Email", "usr1@go.dev"))
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return error", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		_, err := storage.Scan(ctx)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestStorageGSI1(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	storage := dynamorm.NewStorage("TestTable", dynamo)

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
		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName:        aws.String("TestTable"),
				IndexName:        aws.String("GSI1"),
				FilterExpression: aws.String("#Email = :Email"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"},
				},
				ExpressionAttributeNames: map[string]string{
					"#Email": "Email",
				},
			}).
			Return(out, nil)

		query, err := storage.ScanGSI1(ctx, dynamorm.EQ("Email", "usr1@go.dev"))
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return error", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		_, err := storage.ScanGSI1(ctx)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestStorageGSI2(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	storage := dynamorm.NewStorage("TestTable", dynamo)

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
		dynamo.EXPECT().
			Scan(context.TODO(), &dynamodb.ScanInput{
				TableName:        aws.String("TestTable"),
				IndexName:        aws.String("GSI2"),
				FilterExpression: aws.String("#Email = :Email"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"},
				},
				ExpressionAttributeNames: map[string]string{
					"#Email": "Email",
				},
			}).
			Return(out, nil)

		query, err := storage.ScanGSI2(ctx, dynamorm.EQ("Email", "usr1@go.dev"))
		require.NoError(t, err)
		require.Equal(t, int32(1), query.Count())
	})

	t.Run("should return error", func(t *testing.T) {
		dynamo.EXPECT().
			Scan(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		_, err := storage.ScanGSI2(ctx)
		require.ErrorIs(t, err, assert.AnError)
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
		require.EqualError(t, err, "entity pk is empty")
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Remove(context.TODO(), e)
		require.EqualError(t, err, "entity sk is empty")
	})

	t.Run("should return client error", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "SK#1")

		dynamo.EXPECT().
			DeleteItem(gomock.Any(), gomock.Any()).
			Return(nil, assert.AnError)

		err := storage.Remove(context.TODO(), e)
		require.ErrorIs(t, err, assert.AnError)
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
		require.EqualError(t, err, "entity pk is empty")
	})

	t.Run("should return error if sk is empty", func(t *testing.T) {
		e := NewMockEntity(ctrl)
		e.EXPECT().PkSk().Return("PK#1", "")

		err := storage.Update(context.TODO(), e, update)
		require.EqualError(t, err, "entity sk is empty")
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
		require.ErrorIs(t, err, assert.AnError)
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
