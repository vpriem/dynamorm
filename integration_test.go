package dynamorm_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	require.NoError(t, err)
	dynamo := dynamodb.NewFromConfig(cfg, dynamorm.WithBaseEndpoint("http://localhost:8000"))

	tm := dynamorm.NewTableManager(dynamo, tableSchema)
	err = tm.CreateTableIfNotExists(context.TODO())
	require.NoError(t, err)

	storage := dynamorm.NewStorage("TestTable", dynamo)

	t.Run("should test integration", func(t *testing.T) {
		t.Run("should fill up table", func(t *testing.T) {
			count := 10
			for i := 0; i < count; i++ {
				cust := &Customer{}
				ord := &Order{}
				randomize(t, cust, ord)
				err = storage.Save(context.TODO(), cust, ord)
				require.NoError(t, err)
			}
		})

		cust := &Customer{}
		randomize(t, cust)
		cust.Disabled = false
		err := storage.Save(context.TODO(), cust)
		require.NoError(t, err)

		ord1 := &Order{}
		ord2 := &Order{}
		ord3 := &Order{}
		randomize(t, ord1, ord2, ord3)

		ord1.CustomerId = cust.Id
		ord1.Status = "payed"
		ord1.OrderedAt = time.Now()

		ord2.CustomerId = cust.Id
		ord2.Status = "delivered"
		ord2.OrderedAt = time.Now().Add(-1 * 24 * time.Hour)

		ord3.CustomerId = cust.Id
		ord3.Status = "delivered"
		ord3.OrderedAt = time.Now().Add(-2 * 24 * time.Hour)

		err = storage.Save(context.TODO(), ord1, ord2, ord3)
		require.NoError(t, err)

		t.Run("should find customer by id", func(t *testing.T) {
			found := &Customer{
				Id: cust.Id,
			}
			err = storage.Get(context.TODO(), found)
			require.NoError(t, err)
			require.Equal(t, cust.Id, found.Id)
		})

		t.Run("should find all orders by customer id", func(t *testing.T) {
			query, err := storage.QueryGSI1(context.TODO(), fmt.Sprintf("CUSTOMER#%s", cust.Id),
				dynamorm.SkBeginsWith("ORDER#"))
			require.NoError(t, err)
			require.Equal(t, int32(3), query.Count())

			var orders []*Order
			for query.Next(context.TODO()) {
				ord := &Order{}
				err = query.Decode(ord)
				require.NoError(t, err)
				orders = append(orders, ord)
			}
			require.NoError(t, query.Error())

			require.Equal(t, 3, len(orders))
		})

		t.Run("should find all past orders by customer id", func(t *testing.T) {
			query, err := storage.QueryGSI1(context.TODO(), fmt.Sprintf("CUSTOMER#%s", cust.Id),
				dynamorm.SkBeginsWith("ORDER#"),
				dynamorm.LT("OrderedAt", time.Now().Add(-1*24*time.Hour)))
			require.NoError(t, err)
			require.Equal(t, int32(1), query.Count())

			ord := &Order{}
			err = query.First(ord)
			require.NoError(t, err)
			require.Equal(t, ord3.Id, ord.Id)
		})

		t.Run("should find all payed orders by customer id", func(t *testing.T) {
			query, err := storage.QueryGSI1(context.TODO(), fmt.Sprintf("CUSTOMER#%s", cust.Id),
				dynamorm.SkBeginsWith("ORDER#STATUS#payed"))
			require.NoError(t, err)
			require.Equal(t, int32(1), query.Count())

			ord := &Order{}
			err = query.First(ord)
			require.NoError(t, err)
			require.Equal(t, ord1.Id, ord.Id)
		})

		t.Run("should find all delivered orders by customer id", func(t *testing.T) {
			query, err := storage.QueryGSI1(context.TODO(), fmt.Sprintf("CUSTOMER#%s", cust.Id),
				dynamorm.SkBeginsWith("ORDER#STATUS#delivered"))
			require.NoError(t, err)
			require.Equal(t, int32(2), query.Count())
		})
	})
}
