package main

import (
	"fmt"
	"log"

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

	publishCh, err := conn.Channel()

	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	warCh, err := conn.Channel()

	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), pubsub.Transient, handlerMove(gameState, warCh))

	if err != nil {
		fmt.Println("Error subscribing", err)
		return
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), pubsub.Durable, handlerWar(gameState, warCh))

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
			move, err := gameState.CommandMove(arguments)

			if err != nil {
				fmt.Println("Error moving units", err)
			}

			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), move)

			if err != nil {
				fmt.Println("Error publishing", err)
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
