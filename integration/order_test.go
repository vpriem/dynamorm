package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	storage := setUp(t)

	t.Run("should fill up table", func(t *testing.T) {
		entities := make([]dynamorm.Entity, 10)
		for i := 0; i < len(entities); i++ {
			ord := &Order{}
			randomize(t, ord)
			entities[i] = ord
		}

		err := storage.BatchSave(context.TODO(), entities...)
		require.NoError(t, err)
	})

	now := time.Now()

	cust := &Customer{}
	randomize(t, cust)

	ord1 := &Order{}
	ord2 := &Order{}
	ord3 := &Order{}
	ord4 := &Order{}
	randomize(t, ord1, ord2, ord3, ord4)

	ord1.CustomerId = cust.Id
	ord1.Status = "payed"
	ord1.OrderedAt = now

	ord2.CustomerId = cust.Id
	ord2.Status = "delivered"
	ord2.OrderedAt = now.Add(-1 * 24 * time.Hour)

	ord3.CustomerId = cust.Id
	ord3.Status = "delivered"
	ord3.OrderedAt = now.Add(-2 * 24 * time.Hour)

	ord4.CustomerId = cust.Id
	ord4.Status = "cancelled"
	ord4.OrderedAt = now

	err := storage.BatchSave(context.TODO(), ord1, ord2, ord3, ord4)
	require.NoError(t, err)

	t.Run("should find all orders by customer id", func(t *testing.T) {
		pk := fmt.Sprintf("CUSTOMER#%s", cust.Id)
		sk := dynamorm.SkBeginsWith("ORDER#")

		query, err := storage.QueryGSI1(context.TODO(), pk, sk)
		require.NoError(t, err)
		require.Equal(t, int32(4), query.Count())

		expected := []string{
			ord1.Id.String(),
			ord2.Id.String(),
			ord3.Id.String(),
			ord4.Id.String(),
		}

		var orders []string
		for query.Next() {
			ord := &Order{}
			err = query.Decode(ord)
			require.NoError(t, err)
			orders = append(orders, ord.Id.String())
		}
		require.ElementsMatch(t, expected, orders)
	})

	t.Run("should find all orders by customer id with Status=delivered", func(t *testing.T) {
		pk := fmt.Sprintf("CUSTOMER#%s", cust.Id)

		expected := []string{
			ord2.Id.String(),
			ord3.Id.String(),
		}

		t.Run("using SK", func(t *testing.T) {
			sk := dynamorm.SkBeginsWith("ORDER#STATUS#delivered")

			q, err := storage.QueryGSI1(context.TODO(), pk, sk)
			require.NoError(t, err)
			require.Equal(t, int32(2), q.Count())

			var orders []string
			for q.Next() {
				ord := &Order{}
				err = q.Decode(ord)
				require.NoError(t, err)
				orders = append(orders, ord.Id.String())
			}
			require.ElementsMatch(t, expected, orders)
		})

		t.Run("using EQ", func(t *testing.T) {
			sk := dynamorm.SkBeginsWith("ORDER#STATUS")

			q, err := storage.QueryGSI1(context.TODO(), pk, sk, dynamorm.EQ("Status", "delivered"))
			require.NoError(t, err)
			require.Equal(t, int32(2), q.Count())

			var orders []string
			for q.Next() {
				ord := &Order{}
				err = q.Decode(ord)
				require.NoError(t, err)
				orders = append(orders, ord.Id.String())
			}
			require.ElementsMatch(t, expected, orders)
		})
	})

	t.Run("should find all orders by customer id with Status=payed,cancelled", func(t *testing.T) {
		pk := fmt.Sprintf("CUSTOMER#%s", cust.Id)
		sk := dynamorm.SkBeginsWith("ORDER#STATUS")

		expected := []string{
			ord1.Id.String(),
			ord4.Id.String(),
		}

		t.Run("using OR/EQ", func(t *testing.T) {
			q, err := storage.QueryGSI1(context.TODO(), pk, sk,
				dynamorm.OR(
					dynamorm.EQ("Status", "payed"),
					dynamorm.EQ("Status", "cancelled"),
				))
			require.NoError(t, err)
			require.Equal(t, int32(2), q.Count())

			var orders []string
			for q.Next() {
				ord := &Order{}
				err = q.Decode(ord)
				require.NoError(t, err)
				orders = append(orders, ord.Id.String())
			}
			require.ElementsMatch(t, expected, orders)
		})

		t.Run("using IN", func(t *testing.T) {
			q, err := storage.QueryGSI1(context.TODO(), pk, sk,
				dynamorm.IN("Status", "payed", "cancelled"))
			require.NoError(t, err)
			require.Equal(t, int32(2), q.Count())

			var orders []string
			for q.Next() {
				ord := &Order{}
				err = q.Decode(ord)
				require.NoError(t, err)
				orders = append(orders, ord.Id.String())
			}
			require.ElementsMatch(t, expected, orders)
		})
	})
}

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
