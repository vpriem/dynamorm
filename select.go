package dynamorm

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Select creates a Filter that sets the ProjectionExpression to return the specified attributes.
// If called multiple times, the attributes are appended to the existing projection using commas.
func Select(fields ...string) Filter {
	return func(input *Input) {
		if len(fields) == 0 {
			return
		}

		refs := make([]string, len(fields))
		for i, attr := range fields {
			refs[i] = input.refField(attr)
		}

		if input.ProjectionExpression != nil {
			*input.ProjectionExpression = *input.ProjectionExpression + ", " + strings.Join(refs, ", ")
		} else {
			input.ProjectionExpression = aws.String(strings.Join(refs, ", "))
		}
	}
}
