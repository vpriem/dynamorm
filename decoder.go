package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

//go:generate mockgen -package=dynamorm_test -destination=decoder_mock_test.go . DecoderInterface

// DecoderInterface provides a way to decode DynamoDB attribute values into Go types.
type DecoderInterface interface {
	// Decode unmarshals DynamoDB attribute values into the provided out parameter.
	Decode(map[string]types.AttributeValue, interface{}) error
}

type Decoder struct {
	optFns []func(*attributevalue.DecoderOptions)
}

// NewDecoder creates a new Decoder with the provided decoder options.
func NewDecoder(optFns ...func(*attributevalue.DecoderOptions)) *Decoder {
	return &Decoder{optFns}
}

func (e *Decoder) Decode(m map[string]types.AttributeValue, out interface{}) error {
	return attributevalue.UnmarshalMapWithOptions(m, out, e.optFns...)
}

// DefaultDecoder returns a new Decoder with pre-configured options.
func DefaultDecoder() *Decoder {
	return NewDecoder(func(options *attributevalue.DecoderOptions) {
		options.UseEncodingUnmarshalers = true
	})
}
