package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vpriem/dynamorm"
)

func TestCustomer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	storage := setUp(t)

	t.Run("should fill up table", func(t *testing.T) {
		entities := make([]dynamorm.Entity, 10)
		for i := 0; i < len(entities); i++ {
			cust := &Customer{}
			randomize(t, cust)
			entities[i] = cust
		}

		err := storage.BatchSave(context.TODO(), entities...)
		require.NoError(t, err)
	})

	cust1 := &Customer{}
	randomize(t, cust1)
	require.NotEmpty(t, cust1.Id)
	require.NotEmpty(t, cust1.Email)
	err := storage.Save(context.TODO(), cust1)
	require.NoError(t, err)

	t.Run("should find customer by id", func(t *testing.T) {
		found := &Customer{Id: cust1.Id}
		err = storage.Get(context.TODO(), found)
		require.NoError(t, err)
		require.Equal(t, cust1.Id, found.Id)
	})

	t.Run("should find customer by id using SCAN and pagination", func(t *testing.T) {
		filter := expression.Name("Id").Equal(expression.Value(cust1.Id.String()))

		q, err := storage.Scan(context.TODO(),
			dynamorm.ScanFilter(filter),
			dynamorm.ScanLimit(1))
		require.NoError(t, err)

		var found *Customer
		for q.NextPage(context.TODO()) {
			for q.Next() {
				cust := &Customer{}
				err = q.Decode(cust)
				require.NoError(t, err)
				found = cust
			}
		}

		require.NoError(t, q.Error())
		require.NotNil(t, found)
		require.Equal(t, cust1.Id, found.Id)
	})

	t.Run("should find customer by email using GSI", func(t *testing.T) {
		pk := fmt.Sprintf("EMAIL#%s", cust1.Email)

		q, err := storage.QueryGSI1(context.TODO(), pk, nil)
		require.NoError(t, err)
		require.Equal(t, int32(1), q.Count())

		found := &Customer{}
		err = q.First(found)
		require.NoError(t, err)
		require.Equal(t, cust1.Id, found.Id)
	})

	t.Run("should update customer", func(t *testing.T) {
		dst := &Customer{Id: cust1.Id}

		update := expression.Set(
			expression.Name("FirstName"),
			expression.Value("Tacitus"),
		).Set(
			expression.Name("LastName"),
			expression.Value("Kilgore"),
		)

		err := storage.Update(context.TODO(), dst, update,
			dynamorm.UpdateReturnValues(dynamorm.ALL_NEW),
		)
		require.NoError(t, err)

		require.Equal(t, cust1.Id, dst.Id)
		require.Equal(t, cust1.Email, dst.Email)
		require.Equal(t, "Tacitus", dst.FirstName)
		require.Equal(t, "Kilgore", dst.LastName)
	})
}

type Customer struct {
	Id        uuid.UUID `fake:"{uuid}"`
	FirstName string    `fake:"{firstname}"`
	LastName  string    `fake:"{lastname}"`
	Email     string    `fake:"{email}"`
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
	pk := ""
	sk := ""
	if c.Email != "" {
		pk = fmt.Sprintf("EMAIL#%s", c.Email)
		sk = "EMAIL"
	}
	return pk, sk
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
