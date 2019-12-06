// +build integration

package gogo_amqp

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testQueueName    = "test_queue"
	testQueueFailed  = "test_failed"
	testExchangeName = "test_exchange"
)

func setupTest(t *testing.T) (*Consumer, Producer) {
	consumer, err := NewDefaultConsumer()
	require.NoError(t, err)

	producer, err := NewDefaultProducer()
	require.NoError(t, err)

	channel, _ := consumer.conn.Channel()

	err = createQueue(channel, testQueueName)
	require.NoError(t, err)

	err = createQueue(channel, testQueueFailed)
	require.NoError(t, err)

	err = createExchange(channel, testExchangeName)
	require.NoError(t, err)

	err = bindExchangeToQueue(channel, testExchangeName, testQueueName)
	require.NoError(t, err)

	return consumer, producer
}

func TestProduceAndConsumeMessage(t *testing.T) {
	consumer, producer := setupTest(t)

	var triggerCount int
	consumer.RegisterQueueHandler(testQueueName, func(data []byte) error {
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

func TestProduceAndConsumeFailed(t *testing.T) {
	consumer, producer := setupTest(t)

	const maximumRetryCount = 3
	var triggerCount int
	consumer.RegisterQueueHandler(testQueueFailed, func(data []byte) error {
		triggerCount++
		if triggerCount == maximumRetryCount {
			return nil
		}

		return errors.New("test error")
	})

	err := producer.ProduceQueue(testQueueFailed, []byte{})
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
