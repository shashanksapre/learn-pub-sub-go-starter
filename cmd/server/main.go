package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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
	chanelle, err := conn.Channel()

	if err != nil {
		fmt.Println("Error creating channel", err)
		return
	}

	err = internal.PublishJSON(chanelle, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

	if err != nil {
		fmt.Println("Error sending message", err)
		return
	}

	fmt.Println("Connected to RabbitMQ at localhost:5672")
	gamelogic.PrintServerHelp()

	_, _, err = internal.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), internal.Durable)

	if err != nil {
		fmt.Println("Error binding to queue", err)
		return
	}

	for {
		arguments := gamelogic.GetInput()
		if len(arguments) == 0 {
			continue
		}

		if arguments[0] == "pause" {
			fmt.Println("sending a pause")
			continue
		}

		if arguments[0] == "resume" {
			fmt.Println("sending a resume")
			err = internal.PublishJSON(chanelle, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})

			if err != nil {
				fmt.Println("Error sending message", err)
			}

			continue
		}

		if arguments[0] == "quit" {
			fmt.Println("exiting")
			fmt.Println("Peril shutting down...")
			return
		}

		fmt.Println("huh?")
	}
}
