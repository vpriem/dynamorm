package dynamorm

//go:generate mockgen -package=dynamorm_test -destination=entity_mock_test.go . Entity

// Entity is the core interface that must be implemented by any struct
// that will be stored in DynamoDB using this package.
//
// Implementing this interface allows the struct to define how it maps
// to DynamoDB's key schema and provides lifecycle hooks.
type Entity interface {
	// PkSk returns the partition key and sort key for the entity.
	// During a save operation (Save or BatchSave) it is called and saves the values respectively in PK/SK columns.
	// During a Get() operation it is called to know how to retrieve the entity.
	// Both values must be non-empty for operations to succeed.
	PkSk() (string, string)

	// GSI1 returns the partition key and sort key for the first Global Secondary Index.
	// During a save operation (Save or BatchSave) it is called and saves the values respectively in GSI1PK/GSI1SK columns.
	// Return empty strings if not using this GSI.
	GSI1() (string, string)

	// GSI2 returns the partition key and sort key for the second Global Secondary Index.
	// During a save operation (Save or BatchSave) it is called and saves the values respectively in GSI2PK/GSI2SK columns.
	// Return empty strings if not using this GSI.
	GSI2() (string, string)

	// BeforeSave is called before the entity is saved to DynamoDB.
	// This can be used to set timestamps, perform validation, etc.
	// Return an error to abort the save operation.
	BeforeSave() error
}
