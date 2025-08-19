package dynamorm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

func TestScanLimit(t *testing.T) {
	input := &dynamodb.ScanInput{}

	builder := dynamorm.ScanLimit(5)(input, nil)
	require.Nil(t, builder)
	require.NotNil(t, input.Limit)
	require.EqualValues(t, 5, *input.Limit)
}

func TestScanFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	filter := expression.Name("Attr").Equal(expression.Value("value"))

	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithFilter(filter).Return(builder)

	nextBuilder := dynamorm.ScanFilter(filter)(nil, builder)
	require.Equal(t, builder, nextBuilder)
}

func TestScanAttribute(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	proj := expression.NamesList(
		expression.Name("Attr1"),
		expression.Name("Attr2"),
	)

	builder := NewMockBuilderInterface(ctrl)
	builder.EXPECT().WithProjection(proj).Return(builder)

	nextBuilder := dynamorm.ScanAttribute()(nil, builder)
	require.Nil(t, nextBuilder)

	nextBuilder = dynamorm.ScanAttribute("Attr1", "Attr2")(nil, builder)
	require.Equal(t, builder, nextBuilder)
}
