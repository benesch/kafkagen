package main

import (
	"context"
	"math/rand"
	"strconv"

	"github.com/segmentio/kafka-go"
)

const broker = "localhost:9092"
const topic = "upsert-text"

func main() {
	ctx := context.Background()

	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		panic(err)
	}

	conn.DeleteTopics(topic)
	conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     30,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "", ConfigValue: ""},
		},
	})

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
		Async:   true,
	})
	defer w.Close()

	var buf [500]byte
	for i := 0; i < len(buf); i++ {
		buf[i] = 'x'
	}

	for i := 0; i < 1000; i++ {
		w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.FormatInt(rand.Int63n(2_400_000), 10)),
			Value: buf[:],
		})
	}
}
