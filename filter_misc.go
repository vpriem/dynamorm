package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/aws"
)

// Limit creates a Filter that limits the number of results returned by a query or scan.
// It can be used with storage.Query, storage.QueryGSI1, storage.QueryGSI2, and storage.Scan
// to restrict the maximum number of items returned in the results.
func Limit(count int32) Filter {
	return func(input *Input) {
		input.Limit = aws.Int32(count)
	}
}

// ScanIndexForward creates a Filter that controls the sort order of the query results.
// When set to true, the results are returned in ascending order by the sort key; when set to false,
// the results are returned in descending order. This setting applies to Query operations only and
// is ignored for Scan operations.
func ScanIndexForward(forward bool) Filter {
	return func(input *Input) {
		input.ScanIndexForward = aws.Bool(forward)
	}
}

// ConsistentRead creates a Filter that controls whether strongly consistent reads are used.
// When set to true, the operation uses strongly consistent reads; when false, eventually consistent reads are used.
// This setting applies to both Query and Scan operations.
func ConsistentRead(consistent bool) Filter {
	return func(input *Input) {
		input.ConsistentRead = aws.Bool(consistent)
	}
}
