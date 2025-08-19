package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestGetConsistent(t *testing.T) {
	input := &dynamodb.GetItemInput{}
	require.Nil(t, input.ConsistentRead)

	builder := dynamorm.GetConsistent(true)(input, nil)
	require.Nil(t, builder)
	require.NotNil(t, input.ConsistentRead)
	require.True(t, *input.ConsistentRead)

	builder = dynamorm.GetConsistent(false)(input, nil)
	require.Nil(t, builder)
	require.NotNil(t, input.ConsistentRead)
	require.False(t, *input.ConsistentRead)
}

func TestGetAttribute(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	proj := expression.NamesList(
		expression.Name("Attr1"),
		expression.Name("Attr2"),
	)
	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithProjection(proj).Return(builder)

	nextBuilder := dynamorm.GetAttribute()(nil, builder)
	require.Nil(t, nextBuilder)

	nextBuilder = dynamorm.GetAttribute("Attr1", "Attr2")(nil, builder)
	require.Equal(t, builder, nextBuilder)
}
