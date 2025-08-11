package dynamorm_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestQueryInterface(t *testing.T) {
	q := dynamorm.NewQuery(nil, nil, nil, nil)
	var _ dynamorm.QueryInterface = q
}

func TestQueryCount(t *testing.T) {
	q := dynamorm.NewQuery(nil, nil, nil, nil)
	require.Equal(t, int32(0), q.Count())
	require.Equal(t, int32(0), q.ScannedCount())

	out := &dynamodb.QueryOutput{
		Count:        5,
		ScannedCount: 10,
	}
	output := dynamorm.NewOutputFromQueryOutput(out)
	q = dynamorm.NewQuery(nil, nil, output, nil)
	require.Equal(t, int32(5), q.Count())
	require.Equal(t, int32(10), q.ScannedCount())
}

func TestQueryFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)

	t.Run("should decode first item", func(t *testing.T) {
		out := &dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
				{"Email": &types.AttributeValueMemberS{Value: "usr2@go.dev"}},
				{"Email": &types.AttributeValueMemberS{Value: "usr3@go.dev"}},
			},
		}
		output := dynamorm.NewOutputFromQueryOutput(out)

		query := dynamorm.NewQuery(dynamo, nil, output, nil)

		e := &TestEntity{}
		err := query.First(e)
		require.NoError(t, err)
		require.Equal(t, "usr1@go.dev", e.Email)
	})

	t.Run("should return ErrIndexOutOfRange when no items", func(t *testing.T) {
		out := &dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{},
		}
		output := dynamorm.NewOutputFromQueryOutput(out)

		query := dynamorm.NewQuery(dynamo, nil, output, nil)

		err := query.First(nil)
		require.ErrorIs(t, err, dynamorm.ErrIndexOutOfRange)
	})

	t.Run("should return decode error", func(t *testing.T) {
		dec := NewMockDecoderInterface(ctrl)
		dec.EXPECT().Decode(gomock.Any(), gomock.Any()).Return(assert.AnError)

		out := &dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
			},
		}
		output := dynamorm.NewOutputFromQueryOutput(out)

		query := dynamorm.NewQuery(dynamo, nil, output, dec)

		err := query.First(nil)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestQueryLast(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)

	t.Run("should decode last item", func(t *testing.T) {
		out := &dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
				{"Email": &types.AttributeValueMemberS{Value: "usr2@go.dev"}},
				{"Email": &types.AttributeValueMemberS{Value: "usr3@go.dev"}},
			},
		}
		output := dynamorm.NewOutputFromQueryOutput(out)

		query := dynamorm.NewQuery(dynamo, nil, output, nil)

		e := &TestEntity{}
		err := query.Last(e)
		require.NoError(t, err)
		require.Equal(t, "usr3@go.dev", e.Email)
	})

	t.Run("should return ErrIndexOutOfRange when no items", func(t *testing.T) {
		out := &dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{},
		}
		output := dynamorm.NewOutputFromQueryOutput(out)

		query := dynamorm.NewQuery(dynamo, nil, output, nil)
		err := query.Last(nil)
		require.ErrorIs(t, err, dynamorm.ErrIndexOutOfRange)
	})

	t.Run("should return decode error", func(t *testing.T) {
		dec := NewMockDecoderInterface(ctrl)
		dec.EXPECT().Decode(gomock.Any(), gomock.Any()).Return(assert.AnError)

		out := &dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
			},
		}
		output := dynamorm.NewOutputFromQueryOutput(out)

		query := dynamorm.NewQuery(dynamo, nil, output, dec)
		err := query.Last(nil)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestQueryIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	dec := NewMockDecoderInterface(ctrl)

	out := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr0@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr2@go.dev"}},
		},
	}
	output := dynamorm.NewOutputFromQueryOutput(out)

	t.Run("should iterate through all items", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, nil, output, dec)

		dec.EXPECT().Decode(out.Items[0], &TestEntity{}).Return(nil)
		dec.EXPECT().Decode(out.Items[1], &TestEntity{}).Return(nil)
		dec.EXPECT().Decode(out.Items[2], &TestEntity{}).Return(nil)

		var entities []*TestEntity

		for query.Next() {
			e := &TestEntity{}
			err := query.Decode(e)
			require.NoError(t, err)
			entities = append(entities, e)
		}

		require.False(t, query.Next())
		require.Equal(t, 3, len(entities))
	})

	t.Run("should return ErrIndexOutOfRange", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, nil, output, dec)

		err := query.Decode(nil)
		require.ErrorIs(t, err, dynamorm.ErrIndexOutOfRange)

		for query.Next() {
		}

		require.False(t, query.Next())
		err = query.Decode(nil)
		require.ErrorIs(t, err, dynamorm.ErrIndexOutOfRange)
	})

	t.Run("should reset", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, nil, output, dec)
		for query.Next() {
		}

		require.False(t, query.Next())
		query.Reset()
		require.True(t, query.Next())
	})

	t.Run("should return decode error", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, nil, output, dec)

		dec.EXPECT().Decode(gomock.Any(), gomock.Any()).Return(assert.AnError)

		require.True(t, query.Next())
		err := query.Decode(nil)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestQueryPagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	in := &dynamorm.Input{
		TableName: aws.String("Table"),
	}

	out1 := &dynamodb.QueryOutput{
		Count: 3,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr0@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr2@go.dev"}},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"lastKey": &types.AttributeValueMemberS{Value: "1"},
		},
	}
	out2 := &dynamodb.QueryOutput{
		Count: 0,
		Items: []map[string]types.AttributeValue{},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"lastKey": &types.AttributeValueMemberS{Value: "2"},
		},
	}
	out3 := &dynamodb.QueryOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr3@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr4@go.dev"}},
		},
	}

	t.Run("should auto paginate", func(t *testing.T) {
		output := dynamorm.NewOutputFromQueryOutput(out1)
		query := dynamorm.NewQuery(dynamo, in, output, nil)

		dynamo.EXPECT().
			Query(ctx, &dynamodb.QueryInput{
				TableName:         aws.String("Table"),
				ExclusiveStartKey: out1.LastEvaluatedKey,
			}).
			Return(out2, nil)
		dynamo.EXPECT().
			Query(ctx, &dynamodb.QueryInput{
				TableName:         aws.String("Table"),
				ExclusiveStartKey: out2.LastEvaluatedKey,
			}).
			Return(out3, nil)

		var emails []string
		for {
			for query.Next() {
				e := &TestEntity{}
				err := query.Decode(e)
				require.NoError(t, err)
				emails = append(emails, e.Email)
			}

			hasMore, err := query.NextPage(ctx)
			require.NoError(t, err)
			if !hasMore {
				break
			}
		}

		require.Equal(t, int32(1), query.Count())
		require.Equal(t, []string{
			"usr0@go.dev",
			"usr1@go.dev",
			"usr2@go.dev",
			"usr3@go.dev",
			"usr4@go.dev",
		}, emails)
	})

	t.Run("should return auto paginate error ", func(t *testing.T) {
		output := dynamorm.NewOutputFromQueryOutput(out1)
		query := dynamorm.NewQuery(dynamo, in, output, nil)

		dynamo.EXPECT().
			Query(ctx, &dynamodb.QueryInput{
				TableName:         aws.String("Table"),
				ExclusiveStartKey: out1.LastEvaluatedKey,
			}).
			Return(nil, assert.AnError)

		var emails []string
		for {
			for query.Next() {
				e := &TestEntity{}
				err := query.Decode(e)
				require.NoError(t, err)
				emails = append(emails, e.Email)
			}

			hasMore, err := query.NextPage(ctx)
			require.ErrorIs(t, err, assert.AnError)
			require.False(t, hasMore)
			if !hasMore {
				break
			}
		}

		require.Equal(t, []string{
			"usr0@go.dev",
			"usr1@go.dev",
			"usr2@go.dev",
		}, emails)
	})
}

func TestScanPagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	ctx := context.TODO()

	in := &dynamorm.Input{
		TableName: aws.String("Table"),
		IsScan:    true,
	}

	out1 := &dynamodb.ScanOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr0@go.dev"}},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"lastKey": &types.AttributeValueMemberS{Value: "1"},
		},
	}
	out2 := &dynamodb.ScanOutput{
		Count: 0,
		Items: []map[string]types.AttributeValue{},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"lastKey": &types.AttributeValueMemberS{Value: "2"},
		},
	}
	out3 := &dynamodb.ScanOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
		},
	}

	t.Run("should auto paginate", func(t *testing.T) {
		output := dynamorm.NewOutputFromScanOutput(out1)
		query := dynamorm.NewQuery(dynamo, in, output, nil)

		dynamo.EXPECT().
			Scan(ctx, &dynamodb.ScanInput{
				TableName:         aws.String("Table"),
				ExclusiveStartKey: out1.LastEvaluatedKey,
			}).
			Return(out2, nil)
		dynamo.EXPECT().
			Scan(ctx, &dynamodb.ScanInput{
				TableName:         aws.String("Table"),
				ExclusiveStartKey: out2.LastEvaluatedKey,
			}).
			Return(out3, nil)

		var emails []string
		for {
			for query.Next() {
				e := &TestEntity{}
				err := query.Decode(e)
				require.NoError(t, err)
				emails = append(emails, e.Email)
			}

			hasMore, err := query.NextPage(ctx)
			require.NoError(t, err)
			if !hasMore {
				break
			}
		}

		require.Equal(t, int32(1), query.Count())
		require.Equal(t, []string{
			"usr0@go.dev",
			"usr1@go.dev",
		}, emails)
	})

	t.Run("should return auto paginate error ", func(t *testing.T) {
		output := dynamorm.NewOutputFromScanOutput(out1)
		query := dynamorm.NewQuery(dynamo, in, output, nil)

		dynamo.EXPECT().
			Scan(ctx, &dynamodb.ScanInput{
				TableName:         aws.String("Table"),
				ExclusiveStartKey: out1.LastEvaluatedKey,
			}).
			Return(nil, assert.AnError)

		var emails []string
		for {
			for query.Next() {
				e := &TestEntity{}
				err := query.Decode(e)
				require.NoError(t, err)
				emails = append(emails, e.Email)
			}

			hasMore, err := query.NextPage(ctx)
			require.ErrorIs(t, err, assert.AnError)
			require.False(t, hasMore)
			if !hasMore {
				break
			}
		}

		require.Equal(t, []string{
			"usr0@go.dev",
		}, emails)
	})
}
