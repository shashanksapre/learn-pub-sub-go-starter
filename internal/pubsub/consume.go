package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
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

			ackType := handler(target)

			switch ackType {
			case Ack:
				err = delivery.Ack(false)
			case NackRequeue:
				err = delivery.Nack(false, true)
			case NackDiscard:
				err = delivery.Nack(false, false)
			default:
				fmt.Printf("No Acknowledgement")
			}

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

	queue, err := chanelle.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = chanelle.QueueBind(queueName, key, exchange, false, nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return chanelle, queue, nil
}
