package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestRemoveCondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	cond := expression.AttributeExists(expression.Name("Attr"))

	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithCondition(cond).Return(builder)

	nextBuilder := dynamorm.RemoveCondition(cond)(nil, builder)
	require.Equal(t, builder, nextBuilder)
}
