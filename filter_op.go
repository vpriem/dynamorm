package dynamorm

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// OR creates a Filter that combines multiple filters with OR logic.
var OR = createOperator("OR")

// AND creates a Filter that combines multiple filters with AND logic.
var AND = createOperator("AND")

func createOperator(op string) func(filters ...Filter) Filter {
	return func(filters ...Filter) Filter {
		return func(input *dynamodb.QueryInput) {
			if input.ExpressionAttributeNames == nil {
				input.ExpressionAttributeNames = map[string]string{}
			}
			if input.ExpressionAttributeValues == nil {
				input.ExpressionAttributeValues = map[string]types.AttributeValue{}
			}

			var expressions []string

			for _, filter := range filters {
				in := &dynamodb.QueryInput{
					ExpressionAttributeNames:  input.ExpressionAttributeNames,
					ExpressionAttributeValues: input.ExpressionAttributeValues,
				}

				filter(in)

				if in.FilterExpression != nil && *in.FilterExpression != "" {
					expressions = append(expressions, *in.FilterExpression)
				}
			}

			if len(expressions) > 0 {
				expr := strings.Join(expressions, " "+op+" ")
				if len(expressions) > 1 {
					expr = fmt.Sprintf("(%s)", expr)
				}
				if input.FilterExpression != nil {
					expr = fmt.Sprintf("%s AND %s", *input.FilterExpression, expr)
				}
				input.FilterExpression = aws.String(expr)
			}
		}
	}
}
