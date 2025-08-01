package dynamorm_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
	"go.uber.org/mock/gomock"
)

type Order struct {
	Id         uuid.UUID `fake:"{uuid}"`
	CustomerId uuid.UUID `fake:"{uuid}"`
	Status     string    `fake:"{state}"`
	OrderedAt  time.Time
	UpdatedAt  time.Time

	clock func() time.Time `dynamodbav:"-"`
}

func (o *Order) PkSk() (string, string) {
	pk := ""
	sk := ""
	if o.Id != uuid.Nil {
		pk = fmt.Sprintf("ORDER#%s", o.Id)
		sk = "ORDER"
	}
	return pk, sk
}

func (o *Order) GSI1() (string, string) {
	pk := ""
	sk := ""
	if o.CustomerId != uuid.Nil {
		pk = fmt.Sprintf("CUSTOMER#%s", o.CustomerId)
		sk = fmt.Sprintf("ORDER#STATUS#%s", o.Status)
	}
	return pk, sk
}

func (o *Order) GSI2() (string, string) {
	return "", ""
}

func (o *Order) BeforeSave() error {
	clock := o.clock
	if clock == nil {
		clock = time.Now
	}
	o.UpdatedAt = clock()
	return nil
}

func TestOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	storage := dynamorm.NewStorage("TestTable", dynamo)

	orderedAtStr := "2025-08-03T15:55:44.168788+02:00"
	orderedAt, err := time.Parse(time.RFC3339Nano, orderedAtStr)
	require.NoError(t, err)

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	clock := func() time.Time {
		return now
	}

	ord1 := &Order{
		Id:         uuid.MustParse("7036846c-67cc-45f6-993c-f3cf949038b4"),
		CustomerId: uuid.MustParse("3e93bf16-2814-4234-a846-b1e3f43e662b"),
		Status:     "pending",
		OrderedAt:  orderedAt,
		UpdatedAt:  now,
	}

	t.Run("should save order", func(t *testing.T) {
		dynamo.EXPECT().
			PutItem(context.TODO(), &dynamodb.PutItemInput{
				TableName: aws.String("TestTable"),
				Item: map[string]types.AttributeValue{
					"PK":         &types.AttributeValueMemberS{Value: "ORDER#7036846c-67cc-45f6-993c-f3cf949038b4"},
					"SK":         &types.AttributeValueMemberS{Value: "ORDER"},
					"GSI1PK":     &types.AttributeValueMemberS{Value: "CUSTOMER#3e93bf16-2814-4234-a846-b1e3f43e662b"},
					"GSI1SK":     &types.AttributeValueMemberS{Value: "ORDER#STATUS#pending"},
					"Id":         &types.AttributeValueMemberS{Value: "7036846c-67cc-45f6-993c-f3cf949038b4"},
					"CustomerId": &types.AttributeValueMemberS{Value: "3e93bf16-2814-4234-a846-b1e3f43e662b"},
					"Status":     &types.AttributeValueMemberS{Value: "pending"},
					"OrderedAt":  &types.AttributeValueMemberS{Value: orderedAtStr},
					"UpdatedAt":  &types.AttributeValueMemberS{Value: nowStr},
				},
			}).
			Return(&dynamodb.PutItemOutput{}, nil)

		ord := *ord1
		ord.UpdatedAt = time.Time{}
		ord.clock = clock

		err := storage.Save(context.TODO(), &ord)
		require.NoError(t, err)
		require.Equal(t, now, ord.UpdatedAt)
	})

	t.Run("should find one order", func(t *testing.T) {
		dynamo.EXPECT().
			GetItem(context.TODO(), &dynamodb.GetItemInput{
				TableName: aws.String("TestTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "ORDER#7036846c-67cc-45f6-993c-f3cf949038b4"},
					"SK": &types.AttributeValueMemberS{Value: "ORDER"},
				},
			}).
			Return(&dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"Id":         &types.AttributeValueMemberS{Value: "7036846c-67cc-45f6-993c-f3cf949038b4"},
					"CustomerId": &types.AttributeValueMemberS{Value: "3e93bf16-2814-4234-a846-b1e3f43e662b"},
					"Status":     &types.AttributeValueMemberS{Value: "pending"},
					"OrderedAt":  &types.AttributeValueMemberS{Value: orderedAtStr},
					"UpdatedAt":  &types.AttributeValueMemberS{Value: nowStr},
				},
			}, nil)

		ord := &Order{
			Id: uuid.MustParse("7036846c-67cc-45f6-993c-f3cf949038b4"),
		}

		err := storage.Get(context.TODO(), ord)
		require.NoError(t, err)
		require.Equal(t, ord1, ord)
	})
}
