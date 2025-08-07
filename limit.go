package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/aws"
)

// Limit creates a Filter that limits the number of results returned by a query.
// It can be used with storage.Query, storage.QueryGSI1, and storage.QueryGSI2 methods
// to restrict the maximum number of items returned in the query results.
func Limit(count int32) Filter {
	return func(input *Input) {
		input.Limit = aws.Int32(count)
	}
}
