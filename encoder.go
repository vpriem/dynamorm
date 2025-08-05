package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

//go:generate mockgen -package=dynamorm_test -destination=encoder_mock_test.go . EncoderInterface

// EncoderInterface provides a way to encode Go types into DynamoDB attribute values.
type EncoderInterface interface {
	// Encode marshals a Go value into DynamoDB attribute values.
	Encode(interface{}) (map[string]types.AttributeValue, error)
}

type Encoder struct {
	optFns []func(*attributevalue.EncoderOptions)
}

// NewEncoder creates a new Encoder with the provided encoder options.
func NewEncoder(optFns ...func(*attributevalue.EncoderOptions)) *Encoder {
	return &Encoder{optFns}
}

func (e *Encoder) Encode(in interface{}) (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMapWithOptions(in, e.optFns...)
}

// DefaultEncoder returns a new Encoder with pre-configured options.
func DefaultEncoder() *Encoder {
	return NewEncoder(func(options *attributevalue.EncoderOptions) {
		options.OmitEmptyTime = true
		options.UseEncodingMarshalers = true
	})
}
