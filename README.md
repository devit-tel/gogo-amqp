# gogo-amqp
simple amqp client by golang


---

### Todo
- [x] support graceful shutdown
- [x] support direct produce message 
- [x] support exhcnage produce message

---
### Limitation
- support data type only json

---
### Installation
```
    go get -u github.com/devit-tel/gogo-amqp
```
---

### Usage

- create consumer and register queue handler
```go
    consumer, err := gogo_amqp.NewConsumer("localhost:5672", "guest", "guest")
	defer consumer.Close()
	if err != nil {
		panic(err)
	}

	consumer.SetupQueueHandler("test_queue", func(data []byte) error {
        // implement here
		return nil
	})

	consumer.SetupQueueHandler("test_queue_x", func(data []byte) error {
        // implement here
		return nil
	})

	consumer.Start()
```


- create producer and produce message to exchange and direct queue
```go
    producer, err := gogo_amqp.NewProducer("localhost:5672", "guest", "guest")
    if err !=nil {
        panic(err)
    }

    // produce message to exchange
    err = producer.ProduceExchange("test_exchange", jsonData)
    if err != nil {
        panic(err)
    }

    // produce message to direct queue
    err = producer.ProduceQueue("test_queue", jsonData)
    if err != nil {
        panic(err)
    }
```