package dynamorm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestEncoder(t *testing.T) {
	enc := dynamorm.DefaultEncoder()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	e := &TestEntity{
		Id:        uuid.MustParse("3e93bf16-2814-4234-a846-b1e3f43e662b"),
		Email:     "usr1@go.dev",
		Human:     true,
		Age:       25,
		UpdatedAt: now,
	}
	expected := map[string]types.AttributeValue{
		"Id":        &types.AttributeValueMemberS{Value: "3e93bf16-2814-4234-a846-b1e3f43e662b"},
		"Email":     &types.AttributeValueMemberS{Value: "usr1@go.dev"},
		"Human":     &types.AttributeValueMemberBOOL{Value: true},
		"Age":       &types.AttributeValueMemberN{Value: "25"},
		"UpdatedAt": &types.AttributeValueMemberS{Value: nowStr},
	}

	out, err := enc.Encode(e)
	require.NoError(t, err)
	require.Equal(t, expected, out)
}
