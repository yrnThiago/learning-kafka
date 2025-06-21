package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	topic := "meu-topico"
	partition := 0

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Topic:     topic,
		Partition: partition,
		MaxBytes:  10e6, // 10MB
	})

	go func() {
		sign := <-signalChannel
		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
		fmt.Println("Graceful shutdown", sign)
	}()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}
