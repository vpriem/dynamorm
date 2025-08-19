package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestQueryLimit(t *testing.T) {
	input := &dynamodb.QueryInput{}
	require.Nil(t, input.Limit)

	nextBuilder := dynamorm.QueryLimit(10)(input, nil)
	require.Nil(t, nextBuilder)
	require.NotNil(t, input.Limit)
	require.Equal(t, int32(10), *input.Limit)
}

func TestQueryForward(t *testing.T) {
	input := &dynamodb.QueryInput{}
	require.Nil(t, input.ScanIndexForward)

	nextBuilder := dynamorm.QueryForward(true)(input, nil)
	require.Nil(t, nextBuilder)
	require.NotNil(t, input.ScanIndexForward)
	require.Equal(t, true, *input.ScanIndexForward)

	nextBuilder = dynamorm.QueryForward(false)(input, nil)
	require.Nil(t, nextBuilder)
	require.NotNil(t, input.ScanIndexForward)
	require.Equal(t, false, *input.ScanIndexForward)
}

func TestQueryConsistent(t *testing.T) {
	input := &dynamodb.QueryInput{}
	require.Nil(t, input.ConsistentRead)

	nextBuilder := dynamorm.QueryConsistent(true)(input, nil)
	require.Nil(t, nextBuilder)
	require.NotNil(t, input.ConsistentRead)
	require.Equal(t, true, *input.ConsistentRead)

	nextBuilder = dynamorm.QueryConsistent(false)(input, nil)
	require.Nil(t, nextBuilder)
	require.NotNil(t, input.ConsistentRead)
	require.Equal(t, false, *input.ConsistentRead)
}

func TestQueryFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	filter := expression.Name("Attr").Equal(expression.Value("value"))

	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithFilter(filter).Return(builder)

	nextBuilder := dynamorm.QueryFilter(filter)(nil, builder)
	require.Equal(t, builder, nextBuilder)
}

func TestQueryAttribute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proj := expression.NamesList(
		expression.Name("Attr1"),
		expression.Name("Attr2"),
	)

	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithProjection(proj).Return(builder)

	nextBuilder := dynamorm.QueryAttribute()(nil, builder)
	require.Nil(t, nextBuilder)

	nextBuilder = dynamorm.QueryAttribute("Attr1", "Attr2")(nil, builder)
	require.Equal(t, builder, nextBuilder)
}
