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

const AMQP_URI = "amqp://localhost:5675/staging?heartbeat=20,amqp://localhost:5676/staging?heartbeat=20,amqp://localhost:5674/staging?heartbeat=20"

func init() {
	err := setup.SimpleQueueAndExchange("test", "test_queue")
	if err != nil {
		panic(err)
	}
}

func main() {
	producer, err := gr.NewProducerByURI(AMQP_URI, "admin", "admin")
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	lap := 0
	go func() {
		defer wg.Done()

		for {
			err = producer.ProduceExchange("test_queue", &Payload{Message: "hello world", Lap: lap})
			if err != nil {
				log.Println(fmt.Sprintf("%v", err))
				log.Println("retry send")
				time.Sleep(3 * time.Second)
				continue
			}

			log.Printf(" [%d] message sent!", lap)

			lap++
			time.Sleep(time.Second * 2)
		}
	}()

	wg.Wait()
	fmt.Println("Exit Application")
}
