package internal

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)

	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        bytes,
	})

	if err != nil {
		return err
	}

	return nil
}

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

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
