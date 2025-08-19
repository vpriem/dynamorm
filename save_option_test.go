package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestSaveCondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cond := expression.AttributeExists(expression.Name("Attr"))

	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithCondition(cond).Return(builder)

	nextBuilder := dynamorm.SaveCondition(cond)(nil, builder)
	require.Equal(t, builder, nextBuilder)
}
