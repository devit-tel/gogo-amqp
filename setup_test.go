package gogo_amqp

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
