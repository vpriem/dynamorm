package dynamorm

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// IN creates a Filter that adds an IN(...) expression to the filter expression for the given field.
// Example: IN("Status", "PENDING", "ACTIVE") results in: #Status IN (:Status, :Status_1)
var IN = func(field string, values ...interface{}) Filter {
	return func(input *Input) {
		fieldRef := input.refField(field)

		valueRefs := make([]string, len(values))
		for i, value := range values {
			valueRefs[i] = input.refFieldValue(field, value)
		}

		var expr []string
		if input.FilterExpression != nil {
			expr = append(expr, *input.FilterExpression)
		}
		expr = append(expr, fmt.Sprintf("%s IN (%s)", fieldRef, strings.Join(valueRefs, ", ")))

		input.FilterExpression = aws.String(strings.Join(expr, " AND "))
	}
}
