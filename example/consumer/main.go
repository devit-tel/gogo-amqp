package main

import (
	"fmt"
	"time"

	gr "github.com/devit-tel/gogo-rabbitmq"
)

func main() {
	consumer, err := gr.NewConsumer("localhost:5672", "guest", "guest")
	defer consumer.Close()
	if err != nil {
		panic(err)
	}

	consumer.SetupQueueHandler("test_queue", func(data []byte) error {
		fmt.Printf("[test_queue] data: %s \n", string(data))
		time.Sleep(time.Second * 2)
		fmt.Println("[test_queue] end")
		return nil
	})

	consumer.SetupQueueHandler("test_queue_x", func(data []byte) error {
		fmt.Printf("[test_queue_x] data: %s \n", string(data))
		return nil
	})

	consumer.Start()
}
