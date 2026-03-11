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

// This file provides a RabbitMQ priority queue implementation.

package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	db_api "github.com/llm-d-incubation/batch-gateway/internal/database/api"
	amqp "github.com/rabbitmq/amqp091-go"
	"k8s.io/klog/v2"
)

const (
	// Reference time for priority calculation (far in the future)
	// This ensures that earlier SLOs get higher priorities
	referenceDateStr = "2050-01-01T00:00:00Z"

	// Time unit for priority bucketing (1 day in microseconds)
	// This means each priority level represents approximately 1 day
	priorityTimeUnit = int64(24 * time.Hour / time.Microsecond)
)

var (
	referenceTime time.Time
)

func init() {
	var err error
	referenceTime, err = time.Parse(time.RFC3339, referenceDateStr)
	if err != nil {
		panic(fmt.Sprintf("failed to parse reference time: %v", err))
	}
}

// messageWrapper wraps a BatchJobPriority with metadata for internal use.
type messageWrapper struct {
	Job       *db_api.BatchJobPriority `json:"job"`
	EnqueuedAt time.Time                `json:"enqueuedAt"`
}

// calculatePriority converts an SLO timestamp to a RabbitMQ priority (0-maxPriority).
// Earlier SLOs get higher priorities.
func calculatePriority(slo time.Time) uint8 {
	// Calculate difference from reference time in microseconds
	diff := referenceTime.UnixMicro() - slo.UnixMicro()

	// Convert to priority bucket
	priorityBucket := diff / priorityTimeUnit

	// Clamp to valid range [0, maxPriority]
	if priorityBucket < 0 {
		return 0
	}
	if priorityBucket > maxPriority {
		return maxPriority
	}

	return uint8(priorityBucket)
}

// PQEnqueue adds a job priority object to the queue.
func (c *ExchangeDBClientRabbitMQ) PQEnqueue(ctx context.Context, item *db_api.BatchJobPriority) error {
	if ctx == nil {
		ctx = context.Background()
	}
	logger := klog.FromContext(ctx)

	if item == nil {
		err := fmt.Errorf("empty item")
		logger.Error(err, "PQEnqueue:")
		return err
	}

	if err := item.IsValid(); err != nil {
		logger.Error(err, "PQEnqueue: item is invalid")
		return err
	}

	logger = logger.WithValues("ID", item.ID)

	// Get channel (with reconnection if needed)
	channel, err := c.getChannel(ctx)
	if err != nil {
		logger.Error(err, "PQEnqueue: failed to get channel")
		return err
	}

	// Wrap the job with metadata
	wrapper := &messageWrapper{
		Job:        item,
		EnqueuedAt: time.Now(),
	}

	// Marshal to JSON
	body, err := json.Marshal(wrapper)
	if err != nil {
		logger.Error(err, "PQEnqueue: Marshal failed")
		return err
	}

	// Calculate priority based on SLO
	priority := calculatePriority(item.SLO)

	// Prepare message publishing
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
		Priority:     priority,
		Timestamp:    time.Now(),
	}

	// Set expiration if TTL is specified
	if item.TTL > 0 {
		// Convert seconds to milliseconds
		msg.Expiration = fmt.Sprintf("%d", item.TTL*1000)
	}

	// Create context with timeout
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Publish message
	err = channel.PublishWithContext(
		cctx,
		"",                // exchange
		priorityQueueName, // routing key (queue name)
		false,             // mandatory
		false,             // immediate
		msg,
	)
	if err != nil {
		logger.Error(err, "PQEnqueue: PublishWithContext failed")
		return err
	}

	logger.Info("PQEnqueue: succeeded", "priority", priority)
	return nil
}

// PQDequeue atomically removes and returns job priority objects at the head of the queue.
// The function blocks up to the timeout value for a job priority object to be available.
func (c *ExchangeDBClientRabbitMQ) PQDequeue(ctx context.Context, timeout time.Duration, maxItems int) ([]*db_api.BatchJobPriority, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	logger := klog.FromContext(ctx)

	if maxItems <= 0 {
		maxItems = 1
	}

	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Get channel (with reconnection if needed)
	channel, err := c.getChannel(ctx)
	if err != nil {
		logger.Error(err, "PQDequeue: failed to get channel")
		return nil, err
	}

	// Determine the actual timeout to use
	var dequeueCtx context.Context
	var cancel context.CancelFunc

	if timeout > 0 {
		// Use the provided timeout
		dequeueCtx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		// No timeout - try once immediately
		dequeueCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	jobPriorities := make([]*db_api.BatchJobPriority, 0, maxItems)
	startTime := time.Now()

	// Consume messages up to maxItems
	for i := 0; i < maxItems; i++ {
		// Try to get a message
		msg, ok, err := channel.Get(priorityQueueName, true) // auto-ack
		if err != nil {
			logger.Error(err, "PQDequeue: Get failed")
			return nil, err
		}

		if !ok {
			// No message available
			if len(jobPriorities) == 0 && timeout > 0 {
				// Wait a bit before retrying if we haven't gotten any messages yet
				elapsed := time.Since(startTime)
				if elapsed < timeout {
					select {
					case <-dequeueCtx.Done():
						logger.V(2).Info("PQDequeue: no items")
						return nil, nil
					case <-time.After(50 * time.Millisecond):
						// Retry
						i--
						continue
					}
				}
			}
			// No more messages - return what we have or nil
			break
		}

		// Unmarshal the message
		var wrapper messageWrapper
		if err := json.Unmarshal(msg.Body, &wrapper); err != nil {
			logger.Error(err, "PQDequeue: Unmarshal failed")
			return nil, err
		}

		jobPriorities = append(jobPriorities, wrapper.Job)
	}

	if len(jobPriorities) > 0 {
		logger.Info("PQDequeue: succeeded", "nItems", len(jobPriorities))
		return jobPriorities, nil
	}

	logger.V(2).Info("PQDequeue: no items")
	return nil, nil
}

// PQDelete deletes a job priority object from the queue.
// It returns the number of deleted objects.
func (c *ExchangeDBClientRabbitMQ) PQDelete(ctx context.Context, item *db_api.BatchJobPriority) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	logger := klog.FromContext(ctx)

	if item == nil {
		err := fmt.Errorf("empty item")
		logger.Error(err, "PQDelete:")
		return 0, err
	}

	if err := item.IsValid(); err != nil {
		logger.Error(err, "PQDelete: item is invalid")
		return 0, err
	}

	logger = logger.WithValues("ID", item.ID)

	// Get channel (with reconnection if needed)
	channel, err := c.getChannel(ctx)
	if err != nil {
		logger.Error(err, "PQDelete: failed to get channel")
		return 0, err
	}

	// Create context with timeout
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// RabbitMQ doesn't support selective deletion from a queue
	// We need to consume messages, check IDs, and re-queue non-matching ones
	// This is not ideal but matches the interface requirement

	nDeleted := 0
	var messagesToRequeue []amqp.Delivery

	// Limit the number of messages we'll scan to avoid blocking too long
	const maxMessagesToScan = 1000

	// Consume and filter messages until queue is empty
	for i := 0; i < maxMessagesToScan; i++ {
		select {
		case <-cctx.Done():
			// Timeout - re-queue what we have and return
			if err := c.requeueMessages(ctx, channel, messagesToRequeue); err != nil {
				logger.Error(err, "PQDelete: failed to requeue messages after timeout")
			}
			return nDeleted, fmt.Errorf("timeout while deleting")
		default:
		}

		msg, ok, err := channel.Get(priorityQueueName, false) // manual ack
		if err != nil {
			logger.Error(err, "PQDelete: Get failed")
			// Re-queue messages we've already pulled
			if rerr := c.requeueMessages(ctx, channel, messagesToRequeue); rerr != nil {
				logger.Error(rerr, "PQDelete: failed to requeue messages after error")
			}
			return nDeleted, err
		}

		if !ok {
			// No more messages
			break
		}

		// Unmarshal to check ID
		var wrapper messageWrapper
		if err := json.Unmarshal(msg.Body, &wrapper); err != nil {
			logger.Error(err, "PQDelete: Unmarshal failed, nacking message")
			msg.Nack(false, true) // requeue the message
			continue
		}

		// Check if this is the message to delete
		if wrapper.Job.ID == item.ID && wrapper.Job.SLO.Equal(item.SLO) {
			// This is the one to delete - just ack it
			if err := msg.Ack(false); err != nil {
				logger.Error(err, "PQDelete: Ack failed")
			}
			nDeleted++
			logger.Info("PQDelete: found and deleted message")
			// Continue scanning in case there are duplicates
		} else {
			// Not the one to delete - save for re-queuing
			messagesToRequeue = append(messagesToRequeue, msg)
		}
	}

	// Re-queue messages that weren't deleted
	if len(messagesToRequeue) > 0 {
		if err := c.requeueMessages(ctx, channel, messagesToRequeue); err != nil {
			logger.Error(err, "PQDelete: failed to requeue messages")
			return nDeleted, err
		}
	}

	logger.Info("PQDelete: succeeded", "nDeleted", nDeleted)
	return nDeleted, nil
}

// requeueMessages re-publishes messages that were consumed but not deleted.
func (c *ExchangeDBClientRabbitMQ) requeueMessages(ctx context.Context, channel *amqp.Channel, messages []amqp.Delivery) error {
	if len(messages) == 0 {
		return nil
	}

	logger := klog.FromContext(ctx)

	for _, msg := range messages {
		// Re-publish with the same properties
		publishMsg := amqp.Publishing{
			DeliveryMode: msg.DeliveryMode,
			ContentType:  msg.ContentType,
			Body:         msg.Body,
			Priority:     msg.Priority,
			Timestamp:    msg.Timestamp,
			Expiration:   msg.Expiration,
		}

		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		err := channel.PublishWithContext(
			cctx,
			"",
			priorityQueueName,
			false,
			false,
			publishMsg,
		)
		cancel()

		if err != nil {
			logger.Error(err, "requeueMessages: PublishWithContext failed")
			// Nack the original message so it goes back to the queue
			msg.Nack(false, true)
			return err
		}

		// Ack the original message since we successfully re-queued it
		if err := msg.Ack(false); err != nil {
			logger.Error(err, "requeueMessages: Ack failed")
		}
	}

	logger.V(2).Info("requeueMessages: succeeded", "count", len(messages))
	return nil
}
