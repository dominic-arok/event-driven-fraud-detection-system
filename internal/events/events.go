// Package events defines the shared event schemas exchanged over Kafka.
package events

// Transaction is published to the "transactions" topic, keyed by UserID.
type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	UserID        string  `json:"user_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	Location      string  `json:"location"`
	Timestamp     int64   `json:"timestamp"`
}

// FraudAlert is published to the "fraud-alerts" topic.
type FraudAlert struct {
	TransactionID string `json:"transaction_id"`
	UserID        string `json:"user_id"`
	Reason        string `json:"reason"`
	Timestamp     int64  `json:"timestamp"`
}
