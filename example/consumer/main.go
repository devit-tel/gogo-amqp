package main

import (
	"errors"
	"fmt"

	gr "github.com/devit-tel/gogo-amqp"
)

func main() {
	consumer, err := gr.NewConsumer("localhost", "/", "guest", "guest", 5672)
	defer consumer.Close()
	if err != nil {
		panic(err)
	}

	consumer.RegisterQueueHandler("test_queue", func(data []byte) error {
		fmt.Printf("[test_queue] data: %s \n", string(data))
		return errors.New("error with something")
	})

	consumer.RegisterQueueHandler("test_queue_x", func(data []byte) error {
		fmt.Printf("[test_queue_x] data: %s \n", string(data))
		return nil
	})

	consumer.Start()
}
