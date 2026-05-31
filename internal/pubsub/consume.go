package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	chani, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)

	if err != nil {
		return err
	}

	chango, err := chani.Consume(queue.Name, "", false, false, false, false, nil)

	if err != nil {
		return err
	}

	go func() {
		for delivery := range chango {
			var target T
			err := json.Unmarshal(delivery.Body, &target)

			if err != nil {
				fmt.Printf("Error unmarshaling json, %s", err)
			}

			handler(target)
			err = delivery.Ack(false)

			if err != nil {
				fmt.Printf("Error acknowledging delivery, %s", err)
			}
		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	chanelle, err := conn.Channel()

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := chanelle.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = chanelle.QueueBind(queueName, key, exchange, false, nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return chanelle, queue, nil
}
