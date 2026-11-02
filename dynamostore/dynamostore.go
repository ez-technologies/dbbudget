// Package dynamostore is a dbbudget Store on an AWS DynamoDB table with a string partition key "pk" (the budget
// name) and a string sort key "sk" ("slot#<n>", "want#<holder>"). Every change is a conditional write; expiry is
// checked in the condition. Turn on TTL on the "ttl" attribute for cleanup.
package dynamostore

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ez-technologies/dbbudget"
)

// Store is the DynamoDB store.
type Store struct {
	c     *dynamodb.Client
	table string
	name  string
	now   func() time.Time
}

var _ dbbudget.Store = (*Store)(nil)

// New returns the budget name in table (default "dbbudget").
func New(c *dynamodb.Client, table, name string) *Store {
	if table == "" {
		table = "dbbudget"
	}
	if name == "" {
		name = "default"
	}
	return &Store{c: c, table: table, name: name, now: time.Now}
}

// CreateTable makes the table (on-demand) with TTL on "ttl"; for setup scripts and tests.
func (s *Store) CreateTable(ctx context.Context) error {
	_, err := s.c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(s.table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		BillingMode: types.BillingModePayPerRequest,
	})
	var inUse *types.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return err
	}
	_, _ = s.c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{TableName: aws.String(s.table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("ttl"), Enabled: aws.Bool(true)}})
	return nil
}

func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func num(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func (s *Store) key(sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": str(s.name), "sk": str(sk)}
}

func (s *Store) item(sk, holder string, exp time.Time) map[string]types.AttributeValue {
	m := s.key(sk)
	m["holder"], m["expires_ms"], m["ttl"] = str(holder), num(exp.UnixMilli()), num(exp.Add(time.Hour).Unix())
	return m
}

func failed(err error) bool {
	var c *types.ConditionalCheckFailedException
	return errors.As(err, &c)
}

func slotSK(i int) string { return "slot#" + strconv.Itoa(i) }

func (s *Store) Take(ctx context.Context, holder string, max int, ttl time.Duration) (int, error) {
	now := s.now()
	for _, i := range rand.Perm(max) {
		_, err := s.c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: s.item(slotSK(i), holder, now.Add(ttl)),
			ConditionExpression:       aws.String("attribute_not_exists(pk) OR expires_ms < :now"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":now": num(now.UnixMilli())}})
		if failed(err) {
			continue
		}
		if err != nil {
			return -1, err
		}
		_, _ = s.c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(s.table), Key: s.key("want#" + holder)})
		return i, nil
	}
	return -1, nil
}

func (s *Store) Renew(ctx context.Context, holder string, slot int, ttl time.Duration) error {
	_, err := s.c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: s.item(slotSK(slot), holder, s.now().Add(ttl)),
		ConditionExpression:       aws.String("holder = :h"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":h": str(holder)}})
	if failed(err) {
		return dbbudget.ErrLost
	}
	return err
}

func (s *Store) Give(ctx context.Context, holder string, slot int) error {
	_, err := s.c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(s.table), Key: s.key(slotSK(slot)),
		ConditionExpression:       aws.String("holder = :h"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":h": str(holder)}})
	if failed(err) {
		return nil
	}
	return err
}

func (s *Store) Want(ctx context.Context, holder string, ttl time.Duration) error {
	_, err := s.c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: s.item("want#"+holder, holder, s.now().Add(ttl))})
	return err
}

func (s *Store) Wanted(ctx context.Context, holder string) (int, error) {
	out, err := s.c.Query(ctx, &dynamodb.QueryInput{TableName: aws.String(s.table), ConsistentRead: aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :w)"),
		FilterExpression:       aws.String("expires_ms >= :now AND holder <> :h"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": str(s.name), ":w": str("want#"),
			":now": num(s.now().UnixMilli()), ":h": str(holder)},
		Select: types.SelectCount})
	if err != nil {
		return 0, err
	}
	return int(out.Count), nil
}
