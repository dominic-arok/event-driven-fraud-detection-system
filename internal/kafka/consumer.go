package kafka

import (
	"os"
	"strings"

	kafkago "github.com/segmentio/kafka-go"
)

// NewConsumer returns a Reader subscribed to topic as part of groupID.
// Brokers come from the KAFKA_BROKERS env var (comma-separated), falling
// back to the docker-compose external listener.
func NewConsumer(topic, groupID string) *kafkago.Reader {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     strings.Split(brokers, ","),
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafkago.FirstOffset,
	})
}
