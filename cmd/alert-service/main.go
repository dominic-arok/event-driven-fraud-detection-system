package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/joho/godotenv"

	"fraud-detection-system/internal/events"
	"fraud-detection-system/internal/kafka"
)

const (
	fraudAlertsTopic = "fraud-alerts"
	consumerGroup    = "fraud-alert-group"
)

func main() {
	_ = godotenv.Load()

	ctx := context.Background()

	reader := kafka.NewConsumer(fraudAlertsTopic, consumerGroup)
	defer reader.Close()

	log.Println("alert-service: consuming fraud alerts...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("read message failed: %v", err)
		}

		var alert events.FraudAlert
		if err := json.Unmarshal(msg.Value, &alert); err != nil {
			log.Printf("skipping malformed message: %v", err)
			continue
		}

		log.Printf("ALERT: user=%s transaction=%s reason=%q", alert.UserID, alert.TransactionID, alert.Reason)
	}
}
