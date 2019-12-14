package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	gr "github.com/devit-tel/gogo-amqp"
	"github.com/devit-tel/gogo-amqp/example/setup"
)

type Payload struct {
	Message string `json:"message"`
	Lap     int    `json:"lap"`
}

func init() {
	err := setup.SimpleQueueAndExchange("test_exchange", "test_queue")
	if err != nil {
		panic(err)
	}
}

func main() {
	producer, err := gr.NewProducer("localhost:5672", "guest", "guest")

	wg := &sync.WaitGroup{}
	wg.Add(1)

	lap := 0
	go func() {
		defer wg.Done()

		for {
			err = producer.ProduceExchange("test_exchange", &Payload{Message: "hello world", Lap: lap})
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}

			log.Printf(" [%d] message sent!", lap)

			lap++
			time.Sleep(time.Second * 2)
		}
	}()

	wg.Wait()
	fmt.Println("Exit Application")
}
