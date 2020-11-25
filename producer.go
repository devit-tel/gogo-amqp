package gogo_amqp

import (
	"encoding/json"
	"strings"

	"github.com/streadway/amqp"
)

//go:generate mockery -name=Producer
type Producer interface {
	// Produce message support only send to exchange
	ProduceExchange(exchangeName string, data interface{}) error
	ProduceQueue(queueName string, data interface{}) error
}

type ProducerClient struct {
	channel *Channel
}

func NewProducerByURI(uri string, username, password string) (Producer, error) {
	amqpClient, err := NewAMQPClient(strings.Split(uri, ","), username, password)
	if err != nil {
		return nil, err
	}

	return &ProducerClient{
		channel: amqpClient.channel,
	}, nil
}

func NewProducer(uri, vhost, username, password string, port int) (Producer, error) {
	uriPath := &amqp.URI{
		Scheme:   "amqp",
		Host:     uri,
		Port:     port,
		Username: username,
		Password: password,
		Vhost:    vhost,
	}

	conn, err := amqp.Dial(uriPath.String())
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &ProducerClient{
		channel: &Channel{Channel: channel},
	}, nil
}

func NewDefaultProducer() (Producer, error) {
	return NewProducer("localhost", "/", "guest", "guest", 5672)
}

func (pb *ProducerClient) ProduceExchange(exchangeName string, data interface{}) error {
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

func (pb *ProducerClient) ProduceQueue(queueName string, data interface{}) error {
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
