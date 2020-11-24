package gogo_amqp

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

// this libs is reimplement from go-amqp-reconnect for support cluster mode and register handler style
// https://github.com/sirius1024/go-amqp-reconnect

const delay = 3 // reconnect after delay seconds

// ConsumerHandler is a function that subscribe queue. if return an error worker will be send nck and requeue
type ConsumeHandler func(data []byte) error

type Consumer struct {
	conn     *amqp.Connection
	channels map[string]*Channel

	// map queue name with function
	queueHandlers map[string]ConsumeHandler

	waitGroup          *sync.WaitGroup
	channelAllRoutines chan bool

	urls     []string
	username string
	password string
}

// Channel amqp.Channel wrapper
type Channel struct {
	*amqp.Channel
	closed int32
}

func NewConsumerByURI(uri, username, password string) (*Consumer, error) {
	return dialCluster(strings.Split(uri, ","), username, password)
}

func NewConsumer(endpoint, vhost, username, password string, port int) (*Consumer, error) {
	uriPath := &amqp.URI{
		Scheme:   "amqp",
		Host:     endpoint,
		Port:     port,
		Username: username,
		Password: password,
		Vhost:    vhost,
	}

	conn, err := amqp.Dial(uriPath.String())
	if err != nil {

		return nil, err
	}

	return &Consumer{
		conn:               conn,
		channels:           map[string]*Channel{},
		queueHandlers:      map[string]ConsumeHandler{},
		channelAllRoutines: make(chan bool),
		waitGroup:          &sync.WaitGroup{},
		username:           username,
		password:           password,
		urls:               []string{endpoint},
	}, nil
}

func NewDefaultConsumer() (*Consumer, error) {
	return NewConsumer("localhost", "/", "guest", "guest", 5672)
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

func (c *Consumer) RegisterQueueHandler(queueName string, handler ConsumeHandler) {
	c.queueHandlers[queueName] = handler
}

func (c *Consumer) Start() {
	for queueName, queueHandler := range c.queueHandlers {
		channel, err := c.Channel()
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

func dial(endpoint, vhost, username, password string, port int) (*amqp.Connection, error) {
	uriPath := &amqp.URI{
		Scheme:   "amqp",
		Host:     endpoint,
		Port:     port,
		Username: username,
		Password: password,
		Vhost:    vhost,
	}

	return amqp.Dial(uriPath.String())
}

func dialCluster(urls []string, username, password string) (*Consumer, error) {
	nodeSequence := 0

	amqpEndpoint, err := amqp.ParseURI(urls[nodeSequence])
	if err != nil {
		return nil, err
	}

	conn, err := dial(amqpEndpoint.Host, amqpEndpoint.Vhost, username, password, amqpEndpoint.Port)

	if err != nil {
		return nil, err
	}
	consumer := &Consumer{
		conn:               conn,
		channels:           map[string]*Channel{},
		queueHandlers:      map[string]ConsumeHandler{},
		channelAllRoutines: make(chan bool),
		waitGroup:          &sync.WaitGroup{},
		username:           username,
		password:           password,
		urls:               urls,
	}

	go func(urls []string, seq *int) {
		for {
			reason, ok := <-consumer.conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				log.Println("connection closed")
				break
			}
			log.Println(fmt.Sprintf("connection closed, reason: %v", reason))

			// reconnect with another node of cluster
			for {
				time.Sleep(delay * time.Second)

				newSeq := next(urls, *seq)
				*seq = newSeq

				amqpEndpoint, _ := amqp.ParseURI(urls[nodeSequence])
				log.Println(fmt.Sprintf("reconnect at node path %+v", amqpEndpoint))

				conn, err := dial(amqpEndpoint.Host, amqpEndpoint.Vhost, consumer.username, consumer.password, amqpEndpoint.Port)
				if err == nil {
					consumer.conn = conn
					log.Println("reconnect success")
					break
				}

				log.Println(fmt.Sprintf("reconnect failed, err: %v", err))
			}
		}
	}(urls, &nodeSequence)

	return consumer, nil
}

func next(s []string, lastSeq int) int {
	length := len(s)
	if length == 0 || lastSeq == length-1 {
		return 0
	} else if lastSeq < length-1 {
		return lastSeq + 1
	} else {
		return -1
	}
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Consumer) Channel() (*Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				log.Println(fmt.Sprintf("channel closed"))
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			log.Println(fmt.Sprintf("channel closed, reason: %v", reason))

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(delay * time.Second)

				ch, err := c.conn.Channel()
				if err == nil {
					log.Println(fmt.Sprintf("channel recreate success"))
					channel.Channel = ch
					break
				}

				log.Println(fmt.Sprintf("channel recreate failed, err: %v", err))
			}
		}

	}()

	return channel, nil
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Consume warp amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				log.Println(fmt.Sprintf("consume failed, err: %v", err))
				time.Sleep(delay * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}
