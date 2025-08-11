package integration_test

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
)

func randomize(t *testing.T, dsts ...interface{}) {
	for _, dst := range dsts {
		if reflect.ValueOf(dst).Kind() != reflect.Ptr {
			t.Fatalf("randomize: expected a pointer, got %T", dst)
		}

		if err := gofakeit.Struct(dst); err != nil {
			t.Fatal(err)
		}
	}
}

func init() {
	gofakeit.AddFuncLookup("uuid", gofakeit.Info{
		Category:    "custom",
		Description: "Generate a random UUID",
		Output:      "uuid.UUID",
		Generate: func(r *rand.Rand, m *gofakeit.MapParams, info *gofakeit.Info) (interface{}, error) {
			return uuid.New(), nil
		},
	})
}
