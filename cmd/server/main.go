package main

import (
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)

	if err != nil {
		fmt.Println("Error connecting to RabbitMQ", err)
		return
	}

	defer conn.Close()
	fmt.Println("Connected to RabbitMQ at localhost:5672")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Peril shutting down...")
}
