package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"

	"fraud-detection-system/internal/events"
	"fraud-detection-system/internal/kafka"
	"fraud-detection-system/internal/redisclient"
)

const (
	transactionsTopic = "transactions"
	fraudAlertsTopic  = "fraud-alerts"
	consumerGroup     = "fraud-detection-group"

	largeAmountThreshold  = 10000
	rapidTransactionCount = 5
	rapidWindowMs         = 30000
)

func main() {
	_ = godotenv.Load()

	ctx := context.Background()

	rdb := redisclient.New()
	defer rdb.Close()

	reader := kafka.NewConsumer(transactionsTopic, consumerGroup)
	defer reader.Close()

	producer := kafka.NewProducer(fraudAlertsTopic)
	defer producer.Close()

	log.Println("fraud-detection-service: consuming transactions...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("read message failed: %v", err)
		}

		var txn events.Transaction
		if err := json.Unmarshal(msg.Value, &txn); err != nil {
			log.Printf("skipping malformed message: %v", err)
			continue
		}

		for _, reason := range evaluateRules(ctx, rdb, txn) {
			publishAlert(ctx, producer, txn, reason)
		}
	}
}

func evaluateRules(ctx context.Context, rdb *redis.Client, txn events.Transaction) []string {
	var reasons []string

	if txn.Amount > largeAmountThreshold {
		reasons = append(reasons, "Large transaction detected")
	}

	if checkRapidTransactions(ctx, rdb, txn) {
		reasons = append(reasons, "Multiple rapid transactions detected")
	}

	if checkLocationChange(ctx, rdb, txn) {
		reasons = append(reasons, "Unusual location change detected")
	}

	return reasons
}

// checkRapidTransactions records this transaction's timestamp and reports
// whether the latest 5 (or more) recorded timestamps for this user span
// less than 30 seconds.
func checkRapidTransactions(ctx context.Context, rdb *redis.Client, txn events.Transaction) bool {
	key := "user:" + txn.UserID + ":transaction_timestamps"

	if err := rdb.LPush(ctx, key, txn.Timestamp).Err(); err != nil {
		log.Printf("redis LPUSH (timestamps) failed: %v", err)
		return false
	}
	if err := rdb.LTrim(ctx, key, 0, 4).Err(); err != nil {
		log.Printf("redis LTRIM (timestamps) failed: %v", err)
		return false
	}

	raw, err := rdb.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		log.Printf("redis LRANGE (timestamps) failed: %v", err)
		return false
	}
	if len(raw) < rapidTransactionCount {
		return false
	}

	// LPUSH inserts at index 0, so index 0 is newest and the last index is oldest.
	newest, err := strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return false
	}
	oldest, err := strconv.ParseInt(raw[len(raw)-1], 10, 64)
	if err != nil {
		return false
	}

	return newest-oldest < rapidWindowMs
}

// checkLocationChange reports whether the user's previously known location
// differs from the current transaction's location, then always updates the
// stored location to the current one.
func checkLocationChange(ctx context.Context, rdb *redis.Client, txn events.Transaction) bool {
	key := "user:" + txn.UserID + ":location"

	prevLocation, err := rdb.Get(ctx, key).Result()
	changed := err == nil && prevLocation != txn.Location

	if err != nil && err != redis.Nil {
		log.Printf("redis GET (location) failed: %v", err)
	}
	if err := rdb.Set(ctx, key, txn.Location, 0).Err(); err != nil {
		log.Printf("redis SET (location) failed: %v", err)
	}

	return changed
}

func publishAlert(ctx context.Context, producer *kafkago.Writer, txn events.Transaction, reason string) {
	alert := events.FraudAlert{
		TransactionID: txn.TransactionID,
		UserID:        txn.UserID,
		Reason:        reason,
		Timestamp:     time.Now().UnixMilli(),
	}

	payload, err := json.Marshal(alert)
	if err != nil {
		log.Printf("failed to encode fraud alert: %v", err)
		return
	}

	if err := producer.WriteMessages(ctx, kafkago.Message{Value: payload}); err != nil {
		log.Printf("failed to publish fraud alert: %v", err)
	}
}
