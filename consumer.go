package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(
		"meu-topico",
		0,
		sarama.OffsetOldest,
	)

	if err != nil {
		panic(err)
	}

	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	messages := partitionConsumer.Messages()
	for {
		select {
		case msg := <-signalChannel:
			fmt.Printf("Saindo da aplicação (%v)\n", msg)
			return
		case msg := <-messages:
			fmt.Printf("Mensagem recebida %s \n", string(msg.Value))
		}
	}
}
