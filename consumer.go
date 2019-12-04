package gogo_amqp

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/streadway/amqp"
)

// ConsumerHandler is a function that subscribe queue. if return an error worker will be send nck and requeue
type ConsumeHandler func(data []byte) error

type Consumer struct {
	conn     *amqp.Connection
	channels map[string]*amqp.Channel

	// map queue name with function
	queueHandlers map[string]ConsumeHandler

	waitGroup          *sync.WaitGroup
	channelAllRoutines chan bool
}

func NewConsumer(endpoint, username, password string) (*Consumer, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", username, password, endpoint))
	if err != nil {

		return nil, err
	}

	return &Consumer{
		conn:               conn,
		channels:           map[string]*amqp.Channel{},
		queueHandlers:      map[string]ConsumeHandler{},
		channelAllRoutines: make(chan bool),
		waitGroup:          &sync.WaitGroup{},
	}, nil
}

func NewDefaultConsumer() (*Consumer, error) {
	return NewConsumer("localhost:5672", "guest", "guest")
}

func (c *Consumer) Close() {
	for _, channel := range c.channels {
		if channel != nil {
			channel.Close()
		}
	}

	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Consumer) SetupQueueHandler(queueName string, handler ConsumeHandler) {
	c.queueHandlers[queueName] = handler
}

func (c *Consumer) Start() {
	for queueName, queueHandler := range c.queueHandlers {
		channel, err := c.conn.Channel()
		if err != nil {
			panic(err)
		}

		c.channels[queueName] = channel
		c.waitGroup.Add(1)
		go c.startConsume(queueName, c.channels[queueName], queueHandler)
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	close(c.channelAllRoutines)
	c.waitGroup.Wait()
}
