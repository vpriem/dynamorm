package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestReturnValues(t *testing.T) {
	input := &dynamodb.UpdateItemInput{}
	require.Empty(t, input.ReturnValues)

	builder := dynamorm.UpdateReturnValues(dynamorm.ALL_NEW)(input, nil)
	require.Equal(t, types.ReturnValueAllNew, input.ReturnValues)
	require.Nil(t, builder)
}

func TestUpdateCondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	cond := expression.AttributeExists(expression.Name("Attr"))

	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithCondition(cond).Return(builder)

	nextBuilder := dynamorm.UpdateCondition(cond)(nil, builder)
	require.Equal(t, builder, nextBuilder)
}
