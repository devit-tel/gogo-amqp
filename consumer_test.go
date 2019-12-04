package gogo_amqp

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testQueueName    = "test_queue"
	testExchangeName = "test_exchange"
)

func setupTest(t *testing.T) (*Consumer, Producer) {
	consumer, err := NewDefaultConsumer()
	require.NoError(t, err)

	producer, err := NewDefaultProducer()
	require.NoError(t, err)

	channel, _ := consumer.conn.Channel()
	_, err = channel.QueueDeclare(
		testQueueName,
		false,
		false,
		false,
		false,
		nil,
	)
	require.NoError(t, err)

	err = channel.ExchangeDeclare(
		testExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	require.NoError(t, err)

	err = channel.QueueBind(
		testQueueName,
		"",
		testExchangeName,
		false,
		nil,
	)
	require.NoError(t, err)

	return consumer, producer
}

func TestProduceAndConsumeMessage(t *testing.T) {
	consumer, producer := setupTest(t)

	var triggerCount int
	consumer.SetupQueueHandler(testQueueName, func(data []byte) error {
		triggerCount++
		return nil
	})

	err := producer.ProduceExchange(testExchangeName, []byte{})
	require.NoError(t, err)

	err = producer.ProduceExchange(testExchangeName, []byte{})
	require.NoError(t, err)

	err = producer.ProduceQueue(testQueueName, nil)
	require.NoError(t, err)

	go func() {
		time.Sleep(time.Second * 3)
		proc, err := os.FindProcess(os.Getpid())
		require.NoError(t, err)

		err = proc.Signal(os.Interrupt)
		require.NoError(t, err)
	}()
	consumer.Start()

	require.Equal(t, 3, triggerCount)
}
