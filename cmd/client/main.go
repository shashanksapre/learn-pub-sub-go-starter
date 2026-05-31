package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)

	if err != nil {
		fmt.Println("Error connecting to RabbitMQ", err)
		return
	}

	defer conn.Close()
	username, err := gamelogic.ClientWelcome()

	if err != nil {
		fmt.Println("Error reading input", err)
		return
	}

	gameState := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.Transient, handlerPause(gameState))

	if err != nil {
		fmt.Println("Error subscribing", err)
		return
	}

	for {
		arguments := gamelogic.GetInput()
		if len(arguments) == 0 {
			continue
		}

		if arguments[0] == "spawn" {
			err = gameState.CommandSpawn(arguments)

			if err != nil {
				fmt.Println("Error spawning units", err)
			}

			continue
		}

		if arguments[0] == "move" {
			_, err := gameState.CommandMove(arguments)

			if err != nil {
				fmt.Println("Error moving units", err)
			}

			continue
		}

		if arguments[0] == "status" {
			gameState.CommandStatus()
			continue
		}

		if arguments[0] == "help" {
			gamelogic.PrintClientHelp()
			continue
		}

		if arguments[0] == "quit" {
			gamelogic.PrintQuit()
			return
		}

		fmt.Println("huh?")
	}

}
