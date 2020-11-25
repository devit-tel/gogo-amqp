package gogo_amqp

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type AMQPClient struct {
	nodeSequence int
	connection   *amqp.Connection
	channel      *Channel
	delay        int
	urls         []string
	username     string
	password     string
}

func NewAMQPClient(urls []string, username, password string) (*AMQPClient, error) {
	amqpClient := &AMQPClient{
		nodeSequence: 0,
		delay:        3,
		connection:   nil,
		channel:      nil,
		urls:         urls,
		username:     username,
		password:     password,
	}

	conn, err := amqpClient.NewConnection()
	if err != nil {
		panic(fmt.Sprint("[NewAMQPClient.NewConnection]: unable create connection %+v", err))
	}

	amqpClient.connection = conn

	ch, err := amqpClient.NewChannel()
	if err != nil {
		panic(fmt.Sprint("[NewAMQPClient.NewChannel]: unable create channel %+v", err))
	}

	amqpClient.channel = ch

	return amqpClient, nil
}

func (amqpClient *AMQPClient) NewConnection() (*amqp.Connection, error) {
	amqpEndpoint, err := amqp.ParseURI(amqpClient.urls[amqpClient.nodeSequence])
	if err != nil {
		return nil, err
	}

	uriPath := &amqp.URI{
		Scheme:   amqpEndpoint.Scheme,
		Host:     amqpEndpoint.Host,
		Port:     amqpEndpoint.Port,
		Username: amqpClient.username,
		Password: amqpClient.password,
		Vhost:    amqpEndpoint.Vhost,
	}
	fmt.Println(uriPath)

	conn, err := amqp.Dial(uriPath.String())
	if err != nil {
		return nil, err
	}

	amqpClient.connection = conn

	go func(c *AMQPClient) {
		for {
			reason, ok := <-conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				log.Println("[NewAMQPClient.NewConnection]: connection closed")
				break
			}
			log.Println(fmt.Sprintf("[NewAMQPClient.NewConnection]: connection closed, reason: %v", reason))

			// reconnect with another node of cluster
			for {
				time.Sleep(time.Duration(c.delay) * time.Second)

				newSeq := next(c.urls, c.nodeSequence)
				c.nodeSequence = newSeq

				amqpEndpoint, _ := amqp.ParseURI(amqpClient.urls[newSeq])
				log.Println(fmt.Sprintf("[NewAMQPClient.NewConnection]: reconnect at node path %+v", amqpEndpoint))

				conn, err := dial(amqpEndpoint.Host, amqpEndpoint.Vhost, amqpClient.username, amqpClient.password, amqpEndpoint.Port)
				if err == nil {
					amqpClient.connection = conn
					log.Println("[NewAMQPClient.NewConnection]: reconnect success")

					break
				}
				log.Println(fmt.Sprintf("[NewAMQPClient.NewConnection]: reconnect failed, reason: %+v", err))
			}
		}
	}(amqpClient)

	return conn, nil
}

func (amqpClient *AMQPClient) NewChannel() (*Channel, error) {
	channel, err := amqpClient.connection.Channel()
	if err != nil {
		return nil, err
	}

	amqpClient.channel = &Channel{
		Channel: channel,
	}

	go func(c *AMQPClient) {
		for {
			reason, ok := <-amqpClient.channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || c.channel.IsClosed() {
				log.Println("[NewAMQPClient.NewChannel]: channel closed")
				amqpClient.channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			log.Println(fmt.Sprintf("[NewAMQPClient.NewChannel]: channel closed, reason: %v", reason))

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(delay * time.Second)

				ch, err := c.connection.Channel()
				if err == nil {
					log.Println("[NewAMQPClient.NewChannel]: channel recreate success")

					c.channel.Channel = ch
					break
				}

				log.Println(fmt.Sprintf("[NewAMQPClient.NewChannel]: channel recreate failed, reason: %v", err))
			}
		}

	}(amqpClient)

	return amqpClient.channel, nil
}
