package integration_test

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var TableSchema = &dynamodb.CreateTableInput{
	AttributeDefinitions: []types.AttributeDefinition{
		{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("GSI1PK"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("GSI1SK"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("GSI2PK"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("GSI2SK"), AttributeType: types.ScalarAttributeTypeS},
	},
	KeySchema: []types.KeySchemaElement{
		{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
	},
	GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
		{
			IndexName: aws.String("GSI1"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("GSI1SK"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{
				ProjectionType: types.ProjectionTypeAll,
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
		},
		{
			IndexName: aws.String("GSI2"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("GSI2PK"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("GSI2SK"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{
				ProjectionType: types.ProjectionTypeAll,
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
		},
	},
	TableName: aws.String("TestTable"),
	ProvisionedThroughput: &types.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(5),
		WriteCapacityUnits: aws.Int64(5),
	},
}
