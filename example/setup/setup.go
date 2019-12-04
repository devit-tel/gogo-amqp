package setup

import (
	"github.com/streadway/amqp"
)

func createQueue(channel *amqp.Channel, queueName string) error {
	_, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	return err
}

func createExchange(channel *amqp.Channel, exchangeName string) error {
	return channel.ExchangeDeclare(
		exchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
}

func bindExchangeToQueue(channel *amqp.Channel, exchangeName, queueName string) error {
	return channel.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil,
	)
}

// SimpleQueueAndExchange for create queue and config exchange routing
func SimpleQueueAndExchange(exchangeName, queueName string) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if err := createQueue(channel, queueName); err != nil {
		return err
	}

	if err := createExchange(channel, exchangeName); err != nil {
		return err
	}

	if err := bindExchangeToQueue(channel, exchangeName, queueName); err != nil {
		return err
	}

	return nil
}
