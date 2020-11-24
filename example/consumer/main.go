package main

import (
	"errors"
	"fmt"

	gr "github.com/devit-tel/gogo-amqp"
)

const AMQP_URI = "amqp://localhost:5676/staging?heartbeat=20,amqp://localhost:5675/staging?heartbeat=20,amqp://localhost:5674/staging?heartbeat=20"

func main() {
	consumer, err := gr.NewConsumerByURI(AMQP_URI, "admin", "admin")
	defer consumer.Close()
	if err != nil {
		panic(err)
	}

	consumer.RegisterQueueHandler("staging.testQueue", func(data []byte) error {
		fmt.Printf("[staging.testQueue] data: %s \n", string(data))
		return errors.New("error with something")

	})

	consumer.RegisterQueueHandler("test_queue_x", func(data []byte) error {
		fmt.Printf("[test_queue_x] data: %s \n", string(data))
		return nil
	})

	consumer.Start()
}
