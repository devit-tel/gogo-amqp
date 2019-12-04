package gogo_amqp

import (
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

//go:generate mockery -name=Producer
type Producer interface {
	// Produce message support only send to exchange
	ProduceExchange(exchangeName string, data interface{}) error
	ProduceQueue(queueName string, data interface{}) error
}

type Publisher struct {
	channel *amqp.Channel
}

func NewProducer(endpoint, username, password string) (Producer, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", username, password, endpoint))
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Publisher{
		channel: channel,
	}, nil
}

func NewDefaultProducer() (Producer, error) {
	return NewProducer("localhost:5672", "guest", "guest")
}

func (pb *Publisher) ProduceExchange(exchangeName string, data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = pb.channel.Publish(
		exchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	return err
}

func (pb *Publisher) ProduceQueue(queueName string, data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = pb.channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	return err
}
