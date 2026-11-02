package dynamostore_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/ez-technologies/dbbudget"
	"github.com/ez-technologies/dbbudget/budgettest"
	"github.com/ez-technologies/dbbudget/dynamostore"
)

// TestStore runs against DynamoDB Local with placeholder keys; never AWS.
func TestStore(t *testing.T) {
	ep := os.Getenv("DBBUDGET_TEST_DYNAMODB_ENDPOINT")
	if ep == "" {
		t.Skip("DBBUDGET_TEST_DYNAMODB_ENDPOINT is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	c := dynamodb.New(dynamodb.Options{Region: "us-east-1", BaseEndpoint: aws.String(ep),
		Credentials: credentials.NewStaticCredentialsProvider("local", "local", "")})
	if err := dynamostore.New(c, "budgettest", "").CreateTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	budgettest.Store(t, func(t *testing.T) dbbudget.Store {
		return dynamostore.New(c, "budgettest", fmt.Sprintf("b%d", time.Now().UnixNano()))
	})
}
