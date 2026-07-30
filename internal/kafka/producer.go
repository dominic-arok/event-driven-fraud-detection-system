// Package kafka provides thin helpers around segmentio/kafka-go.
package kafka

import (
	"os"
	"strings"

	kafkago "github.com/segmentio/kafka-go"
)

// NewProducer returns a Writer for the given topic using a hash balancer,
// so messages with the same key always land on the same partition. Brokers
// come from the KAFKA_BROKERS env var (comma-separated), falling back to the
// docker-compose external listener.
func NewProducer(topic string) *kafkago.Writer {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}
	return &kafkago.Writer{
		Addr:     kafkago.TCP(strings.Split(brokers, ",")...),
		Topic:    topic,
		Balancer: &kafkago.Hash{},
	}
}
