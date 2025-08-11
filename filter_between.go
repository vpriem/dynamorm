package dynamorm

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// BETWEEN creates a Filter that adds a BETWEEN ... AND ... expression for the given field.
// Example: BETWEEN("Age", 18, 30) results in: #Age BETWEEN :Age AND :Age_1
var BETWEEN = func(field string, start interface{}, end interface{}) Filter {
	return func(input *Input) {
		fieldRef := input.refField(field)
		startRef := input.refFieldValue(field, start)
		endRef := input.refFieldValue(field, end)

		var expr []string
		if input.FilterExpression != nil {
			expr = append(expr, *input.FilterExpression)
		}
		expr = append(expr, fmt.Sprintf("%s BETWEEN %s AND %s", fieldRef, startRef, endRef))

		input.FilterExpression = aws.String(strings.Join(expr, " AND "))
	}
}
