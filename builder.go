package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

//go:generate mockgen -package=dynamorm_test -destination=builder_mock_test.go . BuilderInterface
type BuilderInterface interface {
	WithFilter(expression.ConditionBuilder) BuilderInterface
	WithProjection(expression.ProjectionBuilder) BuilderInterface
	WithUpdate(expression.UpdateBuilder) BuilderInterface
	WithKeyCondition(expression.KeyConditionBuilder) BuilderInterface
	WithCondition(expression.ConditionBuilder) BuilderInterface
	Build() (Expression, error)
}

type Builder struct {
	builder expression.Builder
}

func NewBuilder() BuilderInterface {
	return &Builder{expression.NewBuilder()}
}

func (b *Builder) WithFilter(filter expression.ConditionBuilder) BuilderInterface {
	return &Builder{b.builder.WithFilter(filter)}
}

func (b *Builder) WithProjection(projection expression.ProjectionBuilder) BuilderInterface {
	return &Builder{b.builder.WithProjection(projection)}
}

func (b *Builder) WithCondition(condition expression.ConditionBuilder) BuilderInterface {
	return &Builder{b.builder.WithCondition(condition)}
}

func (b *Builder) WithKeyCondition(keyCond expression.KeyConditionBuilder) BuilderInterface {
	return &Builder{b.builder.WithKeyCondition(keyCond)}
}

func (b *Builder) WithUpdate(update expression.UpdateBuilder) BuilderInterface {
	return &Builder{b.builder.WithUpdate(update)}
}

func (b *Builder) Build() (Expression, error) {
	return b.builder.Build()
}

//go:generate mockgen -package=dynamorm_test -destination=expression_mock_test.go . Expression
type Expression interface {
	Condition() *string
	KeyCondition() *string
	Filter() *string
	Projection() *string
	Names() map[string]string
	Values() map[string]types.AttributeValue
	Update() *string
}
