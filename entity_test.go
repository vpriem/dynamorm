package dynamorm_test

import (
	"time"

	"github.com/google/uuid"
)

type TestEntity struct {
	Id        uuid.UUID
	Email     string
	Age       int
	Human     bool
	UpdatedAt time.Time
}

func (c *TestEntity) PkSk() (string, string) {
	return "", ""
}

func (c *TestEntity) GSI1() (string, string) {
	return "", ""
}

func (c *TestEntity) GSI2() (string, string) {
	return "", ""
}

func (c *TestEntity) BeforeSave() error {
	return nil
}
