/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file provides a RabbitMQ data structures client implementation.

package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	db_api "github.com/llm-d-incubation/batch-gateway/internal/database/api"
	amqp "github.com/rabbitmq/amqp091-go"
	"k8s.io/klog/v2"
)

const (
	priorityQueueName      = "llmd_batch_priority_queue"
	maxPriority            = 10 // RabbitMQ supports 0-255, but 10 is recommended for better performance
	defaultTimeout         = 20 * time.Second
	reconnectDelay         = 5 * time.Second
	maxReconnectAttempts   = 3
	defaultPrefetchCount   = 1
	channelCloseTimeout    = 5 * time.Second
)

var (
	_ db_api.BatchPriorityQueueClient = (*ExchangeDBClientRabbitMQ)(nil)
)

// RabbitMQConfig holds the configuration for RabbitMQ client.
type RabbitMQConfig struct {
	URL         string        // RabbitMQ connection URL (e.g., "amqp://guest:guest@localhost:5672/")
	Timeout     time.Duration // Operation timeout
	ServiceName string        // Service name for logging
}

// DSClientRabbitMQ is the base RabbitMQ client.
type DSClientRabbitMQ struct {
	config     *RabbitMQConfig
	conn       *amqp.Connection
	channel    *amqp.Channel
	timeout    time.Duration
	mu         sync.RWMutex
	closed     bool
	onceClose  *sync.Once
	notifyConn chan *amqp.Error
	notifyChan chan *amqp.Error
}

// ExchangeDBClientRabbitMQ implements the BatchPriorityQueueClient interface.
type ExchangeDBClientRabbitMQ struct {
	*DSClientRabbitMQ
}

// NewExchangeDBClientRabbitMQ creates a new RabbitMQ-based exchange client.
func NewExchangeDBClientRabbitMQ(ctx context.Context, conf *RabbitMQConfig) (*ExchangeDBClientRabbitMQ, error) {
	baseClient, err := NewDSClientRabbitMQ(ctx, conf)
	if err != nil {
		return nil, err
	}
	return &ExchangeDBClientRabbitMQ{DSClientRabbitMQ: baseClient}, nil
}

// NewDSClientRabbitMQ creates a new base RabbitMQ client.
func NewDSClientRabbitMQ(ctx context.Context, conf *RabbitMQConfig) (*DSClientRabbitMQ, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	logger := klog.FromContext(ctx)

	if conf == nil {
		err := fmt.Errorf("empty RabbitMQ config")
		logger.Error(err, "NewDSClientRabbitMQ:")
		return nil, err
	}

	if conf.URL == "" {
		err := fmt.Errorf("RabbitMQ URL is empty")
		logger.Error(err, "NewDSClientRabbitMQ:")
		return nil, err
	}

	timeout := conf.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	client := &DSClientRabbitMQ{
		config:    conf,
		timeout:   timeout,
		onceClose: &sync.Once{},
	}

	// Establish connection
	if err := client.connect(ctx); err != nil {
		return nil, err
	}

	// Declare the priority queue
	if err := client.declarePriorityQueue(ctx); err != nil {
		client.Close()
		return nil, err
	}

	logger.Info("NewDSClientRabbitMQ: succeeded", "serviceName", conf.ServiceName)
	return client, nil
}

// connect establishes a connection to RabbitMQ and creates a channel.
func (c *DSClientRabbitMQ) connect(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	var err error
	c.conn, err = amqp.Dial(c.config.URL)
	if err != nil {
		logger.Error(err, "Failed to connect to RabbitMQ")
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		c.conn.Close()
		logger.Error(err, "Failed to open channel")
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Set up QoS
	if err := c.channel.Qos(defaultPrefetchCount, 0, false); err != nil {
		c.channel.Close()
		c.conn.Close()
		logger.Error(err, "Failed to set QoS")
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Set up notification channels for connection/channel errors
	c.notifyConn = make(chan *amqp.Error, 1)
	c.notifyChan = make(chan *amqp.Error, 1)
	c.conn.NotifyClose(c.notifyConn)
	c.channel.NotifyClose(c.notifyChan)

	return nil
}

// declarePriorityQueue declares the priority queue with the necessary arguments.
func (c *DSClientRabbitMQ) declarePriorityQueue(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	args := amqp.Table{
		"x-max-priority": maxPriority,
	}

	_, err := c.channel.QueueDeclare(
		priorityQueueName, // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		args,              // arguments
	)
	if err != nil {
		logger.Error(err, "Failed to declare priority queue")
		return fmt.Errorf("failed to declare priority queue: %w", err)
	}

	return nil
}

// getChannel returns the current channel, attempting reconnection if necessary.
func (c *DSClientRabbitMQ) getChannel(ctx context.Context) (*amqp.Channel, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("client is closed")
	}

	// Check if channel is still open
	if c.channel != nil {
		c.mu.RUnlock()
		return c.channel, nil
	}
	c.mu.RUnlock()

	// Need to reconnect
	logger := klog.FromContext(ctx)
	logger.Info("Channel is closed, attempting to reconnect")

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}

	if c.channel != nil {
		return c.channel, nil
	}

	// Attempt reconnection
	for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
		logger.Info("Reconnection attempt", "attempt", attempt)

		if err := c.connect(ctx); err != nil {
			logger.Error(err, "Reconnection failed", "attempt", attempt)
			if attempt < maxReconnectAttempts {
				time.Sleep(reconnectDelay)
				continue
			}
			return nil, fmt.Errorf("failed to reconnect after %d attempts: %w", maxReconnectAttempts, err)
		}

		logger.Info("Reconnection successful", "attempt", attempt)
		return c.channel, nil
	}

	return nil, fmt.Errorf("failed to reconnect")
}

// Close closes the RabbitMQ connection and channel.
func (c *DSClientRabbitMQ) Close() error {
	var err error
	c.onceClose.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		// Close channel with timeout
		if c.channel != nil {
			done := make(chan error, 1)
			go func() {
				done <- c.channel.Close()
			}()

			select {
			case err = <-done:
			case <-time.After(channelCloseTimeout):
				err = fmt.Errorf("timeout closing channel")
			}
		}

		// Close connection
		if c.conn != nil {
			if cerr := c.conn.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	})
	return err
}

// IsReady checks if the client is ready to process requests.
func (c *DSClientRabbitMQ) IsReady(ctx context.Context) (ready bool, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false, fmt.Errorf("client is closed")
	}

	if c.conn == nil || c.conn.IsClosed() {
		return false, fmt.Errorf("connection is closed")
	}

	if c.channel == nil {
		return false, fmt.Errorf("channel is nil")
	}

	return true, nil
}
