# gogo-amqp

<p align="left">
  <a href="https://github.com/devit-tel/gogo-amqp"><img alt="GitHub Actions status" src="https://github.com/devit-tel/gogo-amqp/workflows/go-test/badge.svg"></a>
</p>

simple amqp client by golang build on top [github.com/streadway/amqp](https://github.com/streadway/amqp)

---

### Todo

- [x] support graceful shutdown
- [x] support direct produce message
- [x] support exchange produce message
- [x] test failed case
- [x] support cluster nodes
- [x] autoreconnect connection and channel
- [ ] refactor consumer use client same producer
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
- if handler return err gogo-amqp will be send nck and requeue

```go
    	consumer, err := gogo_amqp.NewConsumer("localhost", "/", "guest", "guest", 5672)
	defer consumer.Close()
	if err != nil {
		panic(err)
	}

	consumer.RegisterQueueHandler("test_queue", func(data []byte) error {
        	// implement here
		return nil
	})

	consumer.RegisterQueueHandler("test_queue_x", func(data []byte) error {
        	// implement here
		return nil
	})

	consumer.Start()
```

- create producer and produce message to exchange / direct queue

```go
    producer, err := gogo_amqp.NewProducer("localhost", "/", "guest", "guest", 5672)
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
