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

type Customer struct {
	Id        uuid.UUID `fake:"{uuid}"`
	Email     string    `fake:"{email}"`
	Disabled  bool
	UpdatedAt time.Time

	clock func() time.Time `dynamodbav:"-"`
}

func (c *Customer) PkSk() (string, string) {
	pk := ""
	sk := ""
	if c.Id != uuid.Nil {
		pk = fmt.Sprintf("CUSTOMER#%s", c.Id)
		sk = "CUSTOMER"
	}
	return pk, sk
}

func (c *Customer) GSI1() (string, string) {
	return "", ""
}

func (c *Customer) GSI2() (string, string) {
	return "", ""
}

func (c *Customer) BeforeSave() error {
	clock := c.clock
	if clock == nil {
		clock = time.Now
	}
	c.UpdatedAt = clock()
	return nil
}

func TestCustomer(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	dynamo := NewMockDynamoDB(ctrl)
	storage := dynamorm.NewStorage("TestTable", dynamo)

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	clock := func() time.Time {
		return now
	}

	cust1 := &Customer{
		Id:        uuid.MustParse("3e93bf16-2814-4234-a846-b1e3f43e662b"),
		Email:     "usr1@go.dev",
		UpdatedAt: now,
	}

	t.Run("should save customer", func(t *testing.T) {
		dynamo.EXPECT().
			PutItem(context.TODO(), &dynamodb.PutItemInput{
				TableName: aws.String("TestTable"),
				Item: map[string]types.AttributeValue{
					"PK":        &types.AttributeValueMemberS{Value: "CUSTOMER#3e93bf16-2814-4234-a846-b1e3f43e662b"},
					"SK":        &types.AttributeValueMemberS{Value: "CUSTOMER"},
					"Id":        &types.AttributeValueMemberS{Value: "3e93bf16-2814-4234-a846-b1e3f43e662b"},
					"Email":     &types.AttributeValueMemberS{Value: "usr1@go.dev"},
					"Disabled":  &types.AttributeValueMemberBOOL{Value: false},
					"UpdatedAt": &types.AttributeValueMemberS{Value: nowStr},
				},
			}).
			Return(&dynamodb.PutItemOutput{}, nil)

		cust := *cust1
		cust.UpdatedAt = time.Time{}
		cust.clock = clock

		err := storage.Save(context.TODO(), &cust)
		require.NoError(t, err)
	})

	t.Run("should find one customer", func(t *testing.T) {
		dynamo.EXPECT().
			GetItem(context.TODO(), &dynamodb.GetItemInput{
				TableName: aws.String("TestTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "CUSTOMER#3e93bf16-2814-4234-a846-b1e3f43e662b"},
					"SK": &types.AttributeValueMemberS{Value: "CUSTOMER"},
				},
			}).
			Return(&dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"Id":        &types.AttributeValueMemberS{Value: "3e93bf16-2814-4234-a846-b1e3f43e662b"},
					"Email":     &types.AttributeValueMemberS{Value: "usr1@go.dev"},
					"UpdatedAt": &types.AttributeValueMemberS{Value: nowStr},
				},
			}, nil)

		cust := &Customer{
			Id: uuid.MustParse("3e93bf16-2814-4234-a846-b1e3f43e662b"),
		}

		err := storage.Get(context.TODO(), cust)
		require.NoError(t, err)
		require.Equal(t, cust1, cust)
	})
}
