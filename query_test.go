package dynamorm_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestQueryCount(t *testing.T) {
	q := dynamorm.NewQuery(nil, nil, &dynamodb.QueryOutput{
		Count:        5,
		ScannedCount: 10,
		Items:        []map[string]types.AttributeValue{{}, {}, {}},
	}, nil)
	require.Equal(t, int32(5), q.Count())
	require.Equal(t, int32(10), q.ScannedCount())
	require.Equal(t, 3, q.TotalCount())
}

func TestQueryFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	dec := NewMockDecoderInterface(ctrl)

	in := &dynamodb.QueryInput{}

	out := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{"Attr": &types.AttributeValueMemberS{Value: "str"}},
			{"Attr": &types.AttributeValueMemberBOOL{Value: true}},
			{"Attr": &types.AttributeValueMemberN{Value: "12.34"}},
		},
	}

	query := dynamorm.NewQuery(dynamo, in, out, dec)

	cust := &Customer{}

	t.Run("should decode first item", func(t *testing.T) {
		dec.EXPECT().Decode(out.Items[0], cust).Return(nil)

		err := query.First(cust)
		require.NoError(t, err)
	})

	t.Run("should decode first string item", func(t *testing.T) {
		dec.EXPECT().Decode(out.Items[0], cust).Return(nil)

		err := query.First(cust, dynamorm.FieldValue("Attr", ""))
		require.ErrorIs(t, err, dynamorm.ErrEntityNotFound)
		err = query.First(cust, dynamorm.FieldValue("Attr", "str"))
		require.NoError(t, err)
	})

	t.Run("should decode first bool item", func(t *testing.T) {
		dec.EXPECT().Decode(out.Items[1], cust).Return(nil)

		err := query.First(cust, dynamorm.FieldValue("Attr", false))
		require.ErrorIs(t, err, dynamorm.ErrEntityNotFound)
		err = query.First(cust, dynamorm.FieldValue("Attr", true))
		require.NoError(t, err)
	})

	t.Run("should decode first number item", func(t *testing.T) {
		dec.EXPECT().Decode(out.Items[2], cust).Return(nil)

		err := query.First(cust, dynamorm.FieldValue("Attr", 1.2))
		require.ErrorIs(t, err, dynamorm.ErrEntityNotFound)
		err = query.First(cust, dynamorm.FieldValue("Attr", 12.34))
		require.NoError(t, err)
	})

	t.Run("should return ErrEntityNotFound", func(t *testing.T) {
		err := query.First(cust, dynamorm.FieldValue("Foo", "bar"))
		require.ErrorIs(t, err, dynamorm.ErrEntityNotFound)
	})

	t.Run("should return decode error", func(t *testing.T) {
		dec.EXPECT().Decode(gomock.Any(), gomock.Any()).Return(assert.AnError)

		err := query.First(cust)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestQueryIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	dec := NewMockDecoderInterface(ctrl)

	in := &dynamodb.QueryInput{}

	out := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr0@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr2@go.dev"}},
		},
	}

	ctx := context.TODO()

	t.Run("should iterate through all items", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, in, out, dec)

		dec.EXPECT().Decode(out.Items[0], &Customer{}).Return(nil)
		dec.EXPECT().Decode(out.Items[1], &Customer{}).Return(nil)
		dec.EXPECT().Decode(out.Items[2], &Customer{}).Return(nil)

		var customers []*Customer

		for query.Next(ctx) {
			cust := &Customer{}
			err := query.Decode(cust)
			require.NoError(t, err)
			customers = append(customers, cust)
		}
		require.NoError(t, query.Error())

		require.False(t, query.Next(ctx))
		require.Equal(t, 3, len(customers))
	})

	t.Run("should return ErrIndexOutOfRange", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, in, out, dec)

		err := query.Decode(&Customer{})
		require.ErrorIs(t, err, dynamorm.ErrIndexOutOfRange)

		for query.Next(ctx) {
		}
		require.NoError(t, query.Error())

		require.False(t, query.Next(ctx))
		err = query.Decode(&Customer{})
		require.ErrorIs(t, err, dynamorm.ErrIndexOutOfRange)
	})

	t.Run("should reset", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, in, out, dec)
		for query.Next(ctx) {
		}
		require.NoError(t, query.Error())

		require.False(t, query.Next(ctx))
		query.Reset()
		require.True(t, query.Next(ctx))
	})

	t.Run("should return decode error", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, in, out, dec)

		dec.EXPECT().Decode(gomock.Any(), gomock.Any()).Return(assert.AnError)

		require.True(t, query.Next(ctx))
		err := query.Decode(&Customer{})
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestQueryPagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	dec := NewMockDecoderInterface(ctrl)
	dec.EXPECT().Decode(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ctx := context.TODO()

	in := &dynamodb.QueryInput{}

	out1 := &dynamodb.QueryOutput{
		Count: 3,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr0@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr1@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr2@go.dev"}},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{},
	}
	out2 := &dynamodb.QueryOutput{
		Count:            0,
		Items:            []map[string]types.AttributeValue{},
		LastEvaluatedKey: map[string]types.AttributeValue{},
	}
	out3 := &dynamodb.QueryOutput{
		Count: 1,
		Items: []map[string]types.AttributeValue{
			{"Email": &types.AttributeValueMemberS{Value: "usr3@go.dev"}},
			{"Email": &types.AttributeValueMemberS{Value: "usr4@go.dev"}},
		},
	}

	t.Run("should auto paginate", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, in, out1, dec)

		dynamo.EXPECT().Query(ctx, &dynamodb.QueryInput{ExclusiveStartKey: out1.LastEvaluatedKey}).Return(out2, nil)
		dynamo.EXPECT().Query(ctx, &dynamodb.QueryInput{ExclusiveStartKey: out2.LastEvaluatedKey}).Return(out3, nil)

		var customers []*Customer

		for query.Next(ctx) {
			cust := &Customer{}
			err := query.Decode(cust)
			require.NoError(t, err)
			customers = append(customers, cust)
		}
		require.NoError(t, query.Error())
		require.Equal(t, int32(1), query.Count())
		require.Equal(t, 5, query.TotalCount())
		require.Equal(t, 5, len(customers))
	})

	t.Run("should return auto paginate error ", func(t *testing.T) {
		query := dynamorm.NewQuery(dynamo, in, out1, dec)

		dynamo.EXPECT().Query(ctx, &dynamodb.QueryInput{
			ExclusiveStartKey: out1.LastEvaluatedKey,
		}).Return(nil, assert.AnError)

		var customers []*Customer

		for query.Next(ctx) {
			cust := &Customer{}
			err := query.Decode(cust)
			require.NoError(t, err)
			customers = append(customers, cust)
		}
		require.ErrorIs(t, query.Error(), assert.AnError)
		require.Equal(t, 3, query.TotalCount())
		require.Equal(t, 3, len(customers))

		query.Reset()
		require.NoError(t, query.Error())
	})
}
