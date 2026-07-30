package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"

	"fraud-detection-system/internal/events"
	"fraud-detection-system/internal/kafka"
	"fraud-detection-system/internal/redisclient"
)

const transactionsTopic = "transactions"

type createTransactionRequest struct {
	UserID   string  `json:"user_id" binding:"required"`
	Amount   float64 `json:"amount" binding:"required"`
	Currency string  `json:"currency" binding:"required"`
	Location string  `json:"location" binding:"required"`
}

func main() {
	_ = godotenv.Load() // no-op if .env is absent; real env vars still work

	rdb := redisclient.New()
	defer rdb.Close()

	producer := kafka.NewProducer(transactionsTopic)
	defer producer.Close()

	router := gin.Default()
	router.POST("/api/v1/transactions", createTransactionHandler(rdb, producer))

	if err := router.Run(":3000"); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func createTransactionHandler(rdb *redis.Client, producer *kafkago.Writer) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req createTransactionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		txn := events.Transaction{
			TransactionID: uuid.NewString(),
			UserID:        req.UserID,
			Amount:        req.Amount,
			Currency:      req.Currency,
			Location:      req.Location,
			Timestamp:     time.Now().UnixMilli(),
		}

		payload, err := json.Marshal(txn)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode transaction"})
			return
		}

		ctx := c.Request.Context()

		key := "user:" + txn.UserID + ":transactions"
		if err := rdb.LPush(ctx, key, payload).Err(); err != nil {
			log.Printf("redis LPUSH failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record transaction"})
			return
		}
		if err := rdb.LTrim(ctx, key, 0, 9).Err(); err != nil {
			log.Printf("redis LTRIM failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record transaction"})
			return
		}

		msg := kafkago.Message{
			Key:   []byte(txn.UserID),
			Value: payload,
		}
		if err := producer.WriteMessages(ctx, msg); err != nil {
			log.Printf("kafka publish failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish transaction"})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"success":        true,
			"transaction_id": txn.TransactionID,
		})
	}
}
