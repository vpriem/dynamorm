package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Limit filter for query
func Limit(count int32) Filter {
	return func(input *dynamodb.QueryInput) {
		input.Limit = aws.Int32(count)
	}
}
