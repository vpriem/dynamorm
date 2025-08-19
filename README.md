# DynamORM

[![Go Reference](https://pkg.go.dev/badge/github.com/vpriem/dynamorm.svg)](https://pkg.go.dev/github.com/vpriem/dynamorm)
[![Go Report Card](https://goreportcard.com/badge/github.com/vpriem/dynamorm)](https://goreportcard.com/report/github.com/vpriem/dynamorm)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

DynamORM is a lightweight, flexible Object-Relational Mapping (ORM) library for Amazon DynamoDB in Go. It provides a simple, type-safe way to interact with DynamoDB tables using Go structs.

## Features

- Simple, intuitive API for common DynamoDB operations
- Type-safe mapping between Go structs and DynamoDB items
- Support for primary keys and global secondary indexes (GSIs)
- Composite primary key support and integrity
- Flexible query and filtering capabilities
- Lifecycle hooks for entities
- Automatic pagination
- Built on AWS SDK v2 for Go

## Installation

```bash
go get github.com/vpriem/dynamorm
```

## Quick Start

### Entity

An entity must implement the `Entity` interface:

- `PkSk()`: Returns the partition key and sort key (stored in the PK and SK columns respectively)
- `GSI1()`: Returns the partition key and sort key for the first GSI (stored in GSI1PK and GSI1SK columns)
- `GSI2()`: Returns the partition key and sort key for the second GSI (stored in GSI2PK and GSI2SK columns)
- `BeforeSave()`: Lifecycle hook called before saving the entity, useful for setting timestamps, performing validation, etc.

```go
// User entity example
type User struct {
    ID        uuid.UUID
    Email     string
    Name      string
    UpdatedAt time.Time
}

// PkSk returns the partition key and sort key
func (u *User) PkSk() (string, string) {
    return fmt.Sprintf("USER#%s", u.ID), "USER"
}

// GSI1 returns the partition key and sort key for GSI1
func (u *User) GSI1() (string, string) {
    return "USER#EMAIL", u.Email
}

// GSI2 returns the partition key and sort key for GSI2
func (u *User) GSI2() (string, string) {
    return "", ""
}

// BeforeSave is called before saving the entity
func (u *User) BeforeSave() error {
    u.UpdatedAt = time.Now()
    if !strings.Contains(u.Email, "@") {
        return fmt.Errorf("invalid email: %s", u.Email)
    }
    return nil
}
```

## Storage

### Creating a Storage

```go
// Create DynamoDB client and storage
client := dynamodb.NewFromConfig(cfg)
storage := dynamorm.NewStorage("TableName", client)
```

### Saving an Entity

```go
// Create a new user
user := &User{
    ID:    uuid.MustParse("9be35b9b-e526-404f-8252-e14ce1cb9624"),
    Email: "john@doe.com",
    Name:  "John Doe",
}

// Save the user
err := storage.Save(ctx, user)

// Optionally, save with a condition (conditional write)
cond := expression.AttributeNotExists(expression.Name("PK"))
err = storage.Save(ctx, user, dynamorm.SaveCondition(cond))
```

The entity will be stored in DynamoDB with the following structure:

| PK | SK | GSI1PK | GSI1SK | ID | Email | Name | UpdatedAt |
|----|----|----|----|----|----|----|----|
| `USER#9be35b9b-e526-404f-8252-e14ce1cb9624` | `USER` | `USER#EMAIL` | `john@doe.com` | `9be35b9b-e526-404f-8252-e14ce1cb9624` | `john@doe.com` | `John Doe` | `2025-08-04T10:20:00Z` |

### Batch Saving Entities

```go
// Create users
user1 := &User{ID: uuid.New(), Email: "user1@example.com", Name: "User One"}
user2 := &User{ID: uuid.New(), Email: "user2@example.com", Name: "User Two"}
user3 := &User{ID: uuid.New(), Email: "user3@example.com", Name: "User Three"}

// Batch save all users
err = storage.BatchSave(ctx, user1, user2, user3)
```

### Querying

#### Get One Entity

```go
// Initialize user with ID
user := &User{ID: uuid.MustParse("9be35b9b-e526-404f-8252-e14ce1cb9624")}

// Get user by PK=USER#9be35b9b-e526-404f-8252-e14ce1cb9624 SK=USER
err := storage.Get(ctx, user)
if err != nil {
    if errors.Is(err, dynamorm.ErrEntityNotFound) {
        // Handle not found case
    }
}

fmt.Printf("Found user: %v\n", user)
```

You can customize the underlying GetItem request via options:

- Consistent read: perform a strongly consistent read
- Projection: only fetch specific attributes

```go
// Strongly consistent read, and only fetch Name and Email attributes
if err := storage.Get(ctx, user,
    dynamorm.GetConsistent(true),
    dynamorm.GetAttribute("Name", "Email"),
); err != nil {
    // handle error
}
```

#### Get Multiple Entities

```go
// Find a user using GSI1PK=USER#EMAIL, GSI1SK=john@doe.com
query, err := storage.QueryGSI1(ctx, "USER#EMAIL", dynamorm.SkEQ("john@doe.com"))

// Get the first result of the current page
user := &User{}
err = query.First(user)
if err != nil {
    if errors.Is(err, dynamorm.ErrIndexOutOfRange) {
        // No items on the current page
    }
}

// Get the last result of the current page
user := &User{}
err = query.Last(user)
if err != nil {
    if errors.Is(err, dynamorm.ErrIndexOutOfRange) {
    // No items on the current page
    }
}

// Query with filter conditions: GSI1PK=USER#EMAIL, begins_with(GSI1SK, "john@"), Name="John Doe"
query, err := storage.QueryGSI1(ctx, "USER#EMAIL",
	dynamorm.SkBeginsWith("john@"),
	dynamorm.EQ("Name", "John Doe"))

// Query with filter conditions: GSI1PK=USER#EMAIL, begins_with(GSI1SK, "john@"), Name="John Doe" OR Name="Jane Doe"
query, err := storage.QueryGSI1(ctx, "USER#EMAIL",
	dynamorm.SkBeginsWith("john@"),
    dynamorm.OR(
        dynamorm.EQ("Name", "John Doe"),
        dynamorm.EQ("Name", "Jane Doe"),
    ))
```

Note: `First()` and `Last()` operate on the current page of results. Use `NextPage(ctx)` to fetch subsequent pages.

#### Query Iterator

DynamoDB paginates query results by design. The `Next()` method iterates only through the current page of results.
Similarly, the `Reset()` method only resets the iterator for the current page.

```go
// Iterate through all items of the current query result
for query.Next() {
    user := &User{}
    if err := query.Decode(user); err != nil {
        // Handle decode error
        break
    }
    // Process item
}
```

To retrieve additional results, call `NextPage(ctx)`. Use it as the outer loop and check `query.Error()` after the loop for any pagination error.

```go
// Iterate through all items across all pages
for query.NextPage(ctx) {
    // Process current page
    for query.Next() {
        // Process item
    }
}

if err := query.Error(); err != nil {
    // Handle pagination error
}
```

### Updating an Entity

```go
user := &User{ID: uuid.MustParse("9be35b9b-e526-404f-8252-e14ce1cb9624")}

// Build an update expression using AWS SDK expression
upd := expression.Set(
    expression.Name("Name"),
    expression.Value("Jane Doe"),
)

// Build a condition expression
cond := expression.AttributeExists(expression.Name("Name"))

// Optionally add a condition and request returned attributes
err := storage.Update(ctx, user, upd,
    dynamorm.UpdateCondition(cond),
    dynamorm.UpdateReturnValues(dynamorm.ALL_NEW),
)
if err != nil {
    // Handle error
}
```

This updates specific attributes without overwriting the entire item.
In this example, `dynamorm.ALL_NEW` returns the entire updated item; it will be decoded into the provided entity.

### Removing an Entity

```go
user := &User{ID: uuid.MustParse("9be35b9b-e526-404f-8252-e14ce1cb9624")}

// Optional: apply a condition to the delete (e.g., only delete if Name exists)
cond := expression.AttributeExists(expression.Name("Name"))

err := storage.Remove(ctx, user,
    dynamorm.RemoveCondition(cond),
)
if err != nil {
    // Handle error
}
```

### Batch Removing Entities

```go
// Remove multiple users by their keys
u1 := &User{ID: uuid.MustParse("9be35b9b-e526-404f-8252-e14ce1cb9624")}
u2 := &User{ID: uuid.MustParse("1aa0f2a6-3c68-4f7a-9a74-9a6e7d8c0f21")}

// Batch remove all users (uses PkSk() on each entity)
if err := storage.BatchRemove(ctx, u1, u2); err != nil {
    // Handle error
}
```

Note: DynamoDB's BatchWriteItem is limited to 25 items per request; larger inputs are automatically chunked.


### Transactions

```go
// Create a transaction from storage
tx := storage.Transaction()

// Queue multiple operations atomically
_ = tx.AddSave(user1)
_ = tx.AddConditionCheck(user2, expression.AttributeExists(
	expression.Name("Name"),
))
_ = tx.AddUpdate(user2, expression.Set(
    expression.Name("Name"),
    expression.Value("John Doe Jr."),
))
_ = tx.AddRemove(user3)

// Execute all operations
if err := tx.Execute(ctx); err != nil {
    // Handle error
}
```

## Running Tests

- Unit tests: `make test`
- Integration tests: `make integration` (requires Docker)
- All tests: `make test-all`
- Coverage report: `make coverage`

## License

DynamORM is released under the [MIT License](LICENSE).