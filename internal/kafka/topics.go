package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Partitions per topic. Two lets the two consumer replicas in docker-compose split the load.
const Partitions = 2

// EnsureTopics creates the topics if they do not exist yet, retrying until the broker is
// reachable or ctx expires. Both binaries call it at startup: the producer does not create
// topics on write, and consumer-group readers only see partitions that exist when they join.
func EnsureTopics(ctx context.Context, topics ...string) error {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	var lastErr error
	for {
		if lastErr = createTopics(ctx, broker, topics); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ensure topics: %w (last error: %v)", ctx.Err(), lastErr)
		case <-time.After(time.Second):
		}
	}
}

func createTopics(ctx context.Context, broker string, topics []string) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	cc, err := kafka.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer cc.Close()
	configs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafka.TopicConfig{Topic: t, NumPartitions: Partitions, ReplicationFactor: 1}
	}
	if err := cc.CreateTopics(configs...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return err
	}
	return nil
}
