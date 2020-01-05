package gogo_amqp

import (
	"log"

	"github.com/streadway/amqp"
)

func (c *Consumer) startConsume(queueName string, channel *amqp.Channel, handler ConsumeHandler) error {
	defer c.waitGroup.Done()

	messages, err := channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Unable subscribe queue:%s -> %v", queueName, err)
		return err
	}

	log.Printf("Start subscribe queue: %s", queueName)
	for {
		select {
		case <-c.channelAllRoutines:
			log.Printf("Stop working queue: %s", queueName)
			return nil
		case message := <-messages:
			err = handler(message.Body)
			if err != nil {
				message.Reject(false)
				continue
			}

			message.Ack(false)
		}
	}
}
