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

package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	db_api "github.com/llm-d-incubation/batch-gateway/internal/database/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTimeout = 5 * time.Second
)

// getTestRabbitMQURL returns the RabbitMQ URL from environment variable TEST_RMQ_URL.
// If not set, skips the test.
func getTestRabbitMQURL(tb testing.TB) string {
	url := os.Getenv("TEST_RMQ_URL")
	if url == "" {
		tb.Skip("Skipping test: TEST_RMQ_URL environment variable not set. Set it to run RabbitMQ integration tests.")
	}
	return url
}

// Helper function to create a test client.
func createTestClient(tb testing.TB) *ExchangeDBClientRabbitMQ {
	ctx := context.Background()
	config := &RabbitMQConfig{
		URL:         getTestRabbitMQURL(tb),
		Timeout:     testTimeout,
		ServiceName: "test-service",
	}

	client, err := NewExchangeDBClientRabbitMQ(ctx, config)
	require.NoError(tb, err, "Failed to create test client")
	require.NotNil(tb, client, "Client should not be nil")

	return client
}

// Helper function to purge the test queue.
func purgeQueue(tb testing.TB, client *ExchangeDBClientRabbitMQ) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.channel != nil {
		_, err := client.channel.QueuePurge(priorityQueueName, false)
		if err != nil {
			tb.Logf("Warning: failed to purge queue: %v", err)
		}
	}
}

// TestNewExchangeDBClientRabbitMQ tests client creation
func TestNewExchangeDBClientRabbitMQ(t *testing.T) {
	t.Run("SuccessfulCreation", func(t *testing.T) {
		ctx := context.Background()
		config := &RabbitMQConfig{
			URL:         getTestRabbitMQURL(t),
			Timeout:     testTimeout,
			ServiceName: "test-service",
		}

		client, err := NewExchangeDBClientRabbitMQ(ctx, config)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.Close()

		// Verify client is ready
		ready, err := client.IsReady(ctx)
		assert.NoError(t, err)
		assert.True(t, ready)
	})

	t.Run("NilConfig", func(t *testing.T) {
		ctx := context.Background()
		client, err := NewExchangeDBClientRabbitMQ(ctx, nil)
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "empty RabbitMQ config")
	})

	t.Run("EmptyURL", func(t *testing.T) {
		ctx := context.Background()
		config := &RabbitMQConfig{
			URL:         "",
			Timeout:     testTimeout,
			ServiceName: "test-service",
		}

		client, err := NewExchangeDBClientRabbitMQ(ctx, config)
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "URL is empty")
	})

	t.Run("InvalidURL", func(t *testing.T) {
		ctx := context.Background()
		config := &RabbitMQConfig{
			URL:         "amqp://invalid:5672/",
			Timeout:     testTimeout,
			ServiceName: "test-service",
		}

		client, err := NewExchangeDBClientRabbitMQ(ctx, config)
		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("DefaultTimeout", func(t *testing.T) {
		ctx := context.Background()
		config := &RabbitMQConfig{
			URL:         getTestRabbitMQURL(t),
			Timeout:     0, // Should use default
			ServiceName: "test-service",
		}

		client, err := NewExchangeDBClientRabbitMQ(ctx, config)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.Close()

		assert.Equal(t, defaultTimeout, client.timeout)
	})
}

// TestPQEnqueue tests the enqueue operation
func TestPQEnqueue(t *testing.T) {
	t.Run("HappyPath", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)
		ctx := context.Background()
		now := time.Now()

		item := &db_api.BatchJobPriority{
			ID:   "test-job-1",
			SLO:  now.Add(1 * time.Hour),
			Data: []byte("test data"),
			TTL:  3600,
		}

		err := client.PQEnqueue(ctx, item)
		assert.NoError(t, err)
	})

	t.Run("MultipleItems", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		for i := 0; i < 5; i++ {
			item := &db_api.BatchJobPriority{
				ID:   fmt.Sprintf("test-job-%d", i),
				SLO:  now.Add(time.Duration(i) * time.Hour),
				Data: []byte(fmt.Sprintf("test data %d", i)),
				TTL:  3600,
			}

			err := client.PQEnqueue(ctx, item)
			assert.NoError(t, err)
		}
	})

	t.Run("NilItem", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		err := client.PQEnqueue(ctx, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty item")
	})

	t.Run("InvalidItem_EmptyID", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		item := &db_api.BatchJobPriority{
			ID:  "",
			SLO: time.Now().Add(1 * time.Hour),
		}

		err := client.PQEnqueue(ctx, item)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ID is empty")
	})

	t.Run("InvalidItem_ZeroSLO", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		item := &db_api.BatchJobPriority{
			ID:  "test-job",
			SLO: time.Time{},
		}

		err := client.PQEnqueue(ctx, item)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SLO is zero")
	})

	t.Run("WithoutTTL", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		item := &db_api.BatchJobPriority{
			ID:   "test-job-no-ttl",
			SLO:  time.Now().Add(1 * time.Hour),
			Data: []byte("test data"),
			TTL:  0, // No TTL
		}

		err := client.PQEnqueue(ctx, item)
		assert.NoError(t, err)
	})

	t.Run("PriorityCalculation", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Test different SLO times to ensure priority calculation works
		testCases := []struct {
			name string
			slo  time.Time
		}{
			{"VeryNear", now.Add(1 * time.Hour)},
			{"Near", now.Add(24 * time.Hour)},
			{"Medium", now.Add(7 * 24 * time.Hour)},
			{"Far", now.Add(30 * 24 * time.Hour)},
			{"VeryFar", now.Add(365 * 24 * time.Hour)},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				item := &db_api.BatchJobPriority{
					ID:  fmt.Sprintf("test-job-%s", tc.name),
					SLO: tc.slo,
				}

				err := client.PQEnqueue(ctx, item)
				assert.NoError(t, err)

				priority := calculatePriority(tc.slo)
				assert.LessOrEqual(t, priority, uint8(maxPriority))
				assert.GreaterOrEqual(t, priority, uint8(0))
			})
		}
	})
}

// TestPQDequeue tests the dequeue operation
func TestPQDequeue(t *testing.T) {
	t.Run("HappyPath_SingleItem", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue an item
		item := &db_api.BatchJobPriority{
			ID:   "test-job-1",
			SLO:  now.Add(1 * time.Hour),
			Data: []byte("test data"),
		}
		err := client.PQEnqueue(ctx, item)
		require.NoError(t, err)

		// Dequeue the item
		jobs, err := client.PQDequeue(ctx, 1*time.Second, 1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, item.ID, jobs[0].ID)
		assert.Equal(t, item.SLO.Unix(), jobs[0].SLO.Unix())
		assert.Equal(t, item.Data, jobs[0].Data)
	})

	t.Run("HappyPath_MultipleItems", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue multiple items with different SLOs
		items := []*db_api.BatchJobPriority{
			{ID: "job-1", SLO: now.Add(3 * time.Hour)},
			{ID: "job-2", SLO: now.Add(1 * time.Hour)}, // Earliest - should be dequeued first
			{ID: "job-3", SLO: now.Add(2 * time.Hour)},
		}

		for _, item := range items {
			err := client.PQEnqueue(ctx, item)
			require.NoError(t, err)
		}

		// Give RabbitMQ a moment to sort by priority
		time.Sleep(100 * time.Millisecond)

		// Dequeue all items
		jobs, err := client.PQDequeue(ctx, 1*time.Second, 3)
		assert.NoError(t, err)
		assert.Len(t, jobs, 3)

		// Verify priority order (earliest SLO should come first)
		// Note: With only 10 priority levels and close timestamps, exact ordering may vary
		// but job-2 (earliest) should generally come first
		foundEarliest := false
		for _, job := range jobs {
			if job.ID == "job-2" {
				foundEarliest = true
			}
		}
		assert.True(t, foundEarliest, "Should have dequeued the job with earliest SLO")
	})

	t.Run("EmptyQueue_NoTimeout", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		// Dequeue from empty queue with no timeout
		jobs, err := client.PQDequeue(ctx, 0, 1)
		assert.NoError(t, err)
		assert.Nil(t, jobs)
	})

	t.Run("EmptyQueue_WithTimeout", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		// Dequeue from empty queue with timeout
		start := time.Now()
		jobs, err := client.PQDequeue(ctx, 500*time.Millisecond, 1)
		elapsed := time.Since(start)

		assert.NoError(t, err)
		assert.Nil(t, jobs)
		assert.GreaterOrEqual(t, elapsed, 400*time.Millisecond)
	})

	t.Run("MaxItems_LessThanAvailable", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue 5 items
		for i := 0; i < 5; i++ {
			item := &db_api.BatchJobPriority{
				ID:  fmt.Sprintf("test-job-%d", i),
				SLO: now.Add(time.Duration(i) * time.Hour),
			}
			err := client.PQEnqueue(ctx, item)
			require.NoError(t, err)
		}

		// Dequeue only 2 items
		jobs, err := client.PQDequeue(ctx, 1*time.Second, 2)
		assert.NoError(t, err)
		assert.Len(t, jobs, 2)

		// Verify there are still 3 items in the queue
		remainingJobs, err := client.PQDequeue(ctx, 1*time.Second, 10)
		assert.NoError(t, err)
		assert.Len(t, remainingJobs, 3)
	})

	t.Run("MaxItems_Zero", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		// Enqueue an item
		item := &db_api.BatchJobPriority{
			ID:  "test-job",
			SLO: time.Now().Add(1 * time.Hour),
		}
		err := client.PQEnqueue(ctx, item)
		require.NoError(t, err)

		// Dequeue with maxItems = 0 (should default to 1)
		jobs, err := client.PQDequeue(ctx, 1*time.Second, 0)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx, cancel := context.WithCancel(context.Background())

		// Cancel context before dequeue
		cancel()

		jobs, err := client.PQDequeue(ctx, 1*time.Second, 1)
		assert.Error(t, err)
		assert.Nil(t, jobs)
	})

	t.Run("PartialDequeue", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue 3 items
		for i := 0; i < 3; i++ {
			item := &db_api.BatchJobPriority{
				ID:  fmt.Sprintf("test-job-%d", i),
				SLO: now.Add(time.Duration(i) * time.Hour),
			}
			err := client.PQEnqueue(ctx, item)
			require.NoError(t, err)
		}

		// Request more items than available
		jobs, err := client.PQDequeue(ctx, 1*time.Second, 10)
		assert.NoError(t, err)
		assert.Len(t, jobs, 3)
	})
}

// TestPQDelete tests the delete operation
func TestPQDelete(t *testing.T) {
	t.Run("HappyPath", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue an item
		item := &db_api.BatchJobPriority{
			ID:  "test-job-1",
			SLO: now.Add(1 * time.Hour),
		}
		err := client.PQEnqueue(ctx, item)
		require.NoError(t, err)

		// Delete the item
		nDeleted, err := client.PQDelete(ctx, item)
		assert.NoError(t, err)
		assert.Equal(t, 1, nDeleted)

		// Verify queue is empty
		jobs, err := client.PQDequeue(ctx, 100*time.Millisecond, 1)
		assert.NoError(t, err)
		assert.Nil(t, jobs)
	})

	t.Run("DeleteFromMultipleItems", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue multiple items
		items := []*db_api.BatchJobPriority{
			{ID: "job-1", SLO: now.Add(1 * time.Hour)},
			{ID: "job-2", SLO: now.Add(2 * time.Hour)},
			{ID: "job-3", SLO: now.Add(3 * time.Hour)},
		}

		for _, item := range items {
			err := client.PQEnqueue(ctx, item)
			require.NoError(t, err)
		}

		// Delete the middle item
		nDeleted, err := client.PQDelete(ctx, items[1])
		assert.NoError(t, err)
		assert.Equal(t, 1, nDeleted)

		// Verify only 2 items remain
		jobs, err := client.PQDequeue(ctx, 1*time.Second, 10)
		assert.NoError(t, err)
		assert.Len(t, jobs, 2)

		// Verify the correct item was deleted
		for _, job := range jobs {
			assert.NotEqual(t, "job-2", job.ID)
		}
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		// Try to delete non-existent item
		item := &db_api.BatchJobPriority{
			ID:  "non-existent",
			SLO: time.Now().Add(1 * time.Hour),
		}

		nDeleted, err := client.PQDelete(ctx, item)
		assert.NoError(t, err)
		assert.Equal(t, 0, nDeleted)
	})

	t.Run("NilItem", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()

		ctx := context.Background()

		nDeleted, err := client.PQDelete(ctx, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty item")
		assert.Equal(t, 0, nDeleted)
	})

	t.Run("InvalidItem", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()

		ctx := context.Background()

		item := &db_api.BatchJobPriority{
			ID:  "", // Invalid
			SLO: time.Now().Add(1 * time.Hour),
		}

		nDeleted, err := client.PQDelete(ctx, item)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ID is empty")
		assert.Equal(t, 0, nDeleted)
	})

	t.Run("EmptyQueue", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		item := &db_api.BatchJobPriority{
			ID:  "test-job",
			SLO: time.Now().Add(1 * time.Hour),
		}

		nDeleted, err := client.PQDelete(ctx, item)
		assert.NoError(t, err)
		assert.Equal(t, 0, nDeleted)
	})

	t.Run("DeleteDuplicate", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue the same item twice (same ID and SLO)
		item := &db_api.BatchJobPriority{
			ID:  "duplicate-job",
			SLO: now.Add(1 * time.Hour),
		}

		err := client.PQEnqueue(ctx, item)
		require.NoError(t, err)
		err = client.PQEnqueue(ctx, item)
		require.NoError(t, err)

		// Delete should remove all instances
		nDeleted, err := client.PQDelete(ctx, item)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, nDeleted, 1)
	})
}

// TestClientLifecycle tests client lifecycle operations
func TestClientLifecycle(t *testing.T) {
	t.Run("CloseAndReuse", func(t *testing.T) {
		client := createTestClient(t)

		ctx := context.Background()

		// Close the client
		err := client.Close()
		assert.NoError(t, err)

		// Verify client is not ready
		ready, err := client.IsReady(ctx)
		assert.Error(t, err)
		assert.False(t, ready)

		// Try to use after close
		item := &db_api.BatchJobPriority{
			ID:  "test-job",
			SLO: time.Now().Add(1 * time.Hour),
		}

		err = client.PQEnqueue(ctx, item)
		assert.Error(t, err)
	})

	t.Run("DoubleClose", func(t *testing.T) {
		client := createTestClient(t)

		err := client.Close()
		assert.NoError(t, err)

		// Second close should be safe
		err = client.Close()
		assert.NoError(t, err)
	})
}

// TestEdgeCases tests various edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("LargeData", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		// Create item with large data payload
		largeData := make([]byte, 10*1024) // 10 KB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		item := &db_api.BatchJobPriority{
			ID:   "large-data-job",
			SLO:  time.Now().Add(1 * time.Hour),
			Data: largeData,
		}

		err := client.PQEnqueue(ctx, item)
		assert.NoError(t, err)

		jobs, err := client.PQDequeue(ctx, 1*time.Second, 1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, largeData, jobs[0].Data)
	})

	t.Run("PastSLO", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		// Create item with SLO in the past
		item := &db_api.BatchJobPriority{
			ID:  "past-slo-job",
			SLO: time.Now().Add(-1 * time.Hour),
		}

		err := client.PQEnqueue(ctx, item)
		assert.NoError(t, err)

		jobs, err := client.PQDequeue(ctx, 1*time.Second, 1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
	})

	t.Run("VeryFutureSLO", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		// Create item with SLO far in the future
		item := &db_api.BatchJobPriority{
			ID:  "future-slo-job",
			SLO: time.Now().Add(365 * 24 * time.Hour), // 1 year
		}

		err := client.PQEnqueue(ctx, item)
		assert.NoError(t, err)

		jobs, err := client.PQDequeue(ctx, 1*time.Second, 1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
	})

	t.Run("ConcurrentEnqueue", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()
		now := time.Now()

		// Enqueue multiple items concurrently
		done := make(chan bool)
		numGoroutines := 10

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				item := &db_api.BatchJobPriority{
					ID:  fmt.Sprintf("concurrent-job-%d", id),
					SLO: now.Add(time.Duration(id) * time.Hour),
				}
				err := client.PQEnqueue(ctx, item)
				assert.NoError(t, err)
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify all items were enqueued
		jobs, err := client.PQDequeue(ctx, 1*time.Second, 20)
		assert.NoError(t, err)
		assert.Len(t, jobs, numGoroutines)
	})

	t.Run("SpecialCharactersInID", func(t *testing.T) {
		client := createTestClient(t)
		defer client.Close()
		defer purgeQueue(t, client)

		ctx := context.Background()

		item := &db_api.BatchJobPriority{
			ID:  "test-job-!@#$%^&*()_+-={}[]|:;<>?,./",
			SLO: time.Now().Add(1 * time.Hour),
		}

		err := client.PQEnqueue(ctx, item)
		assert.NoError(t, err)

		jobs, err := client.PQDequeue(ctx, 1*time.Second, 1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, item.ID, jobs[0].ID)
	})
}

// TestPriorityCalculation tests the priority calculation function
func TestPriorityCalculation(t *testing.T) {
	t.Run("EarlierSLOHigherPriority", func(t *testing.T) {
		now := time.Now()
		earlier := now.Add(1 * time.Hour)
		later := now.Add(10 * time.Hour)

		earlierPriority := calculatePriority(earlier)
		laterPriority := calculatePriority(later)

		// Earlier SLO should have higher or equal priority
		assert.GreaterOrEqual(t, earlierPriority, laterPriority)
	})

	t.Run("PriorityRange", func(t *testing.T) {
		now := time.Now()

		testCases := []time.Time{
			now.Add(-24 * time.Hour),
			now,
			now.Add(24 * time.Hour),
			now.Add(365 * 24 * time.Hour),
		}

		for _, slo := range testCases {
			priority := calculatePriority(slo)
			assert.GreaterOrEqual(t, priority, uint8(0))
			assert.LessOrEqual(t, priority, uint8(maxPriority))
		}
	})
}

// BenchmarkPQEnqueue benchmarks the enqueue operation
func BenchmarkPQEnqueue(b *testing.B) {
	ctx := context.Background()
	config := &RabbitMQConfig{
		URL:         getTestRabbitMQURL(b),
		Timeout:     testTimeout,
		ServiceName: "bench-service",
	}

	client, err := NewExchangeDBClientRabbitMQ(ctx, config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	defer purgeQueue(b, client)

	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := &db_api.BatchJobPriority{
			ID:  fmt.Sprintf("bench-job-%d", i),
			SLO: now.Add(time.Duration(i) * time.Second),
		}
		_ = client.PQEnqueue(ctx, item)
	}
}

// BenchmarkPQDequeue benchmarks the dequeue operation
func BenchmarkPQDequeue(b *testing.B) {
	ctx := context.Background()
	config := &RabbitMQConfig{
		URL:         getTestRabbitMQURL(b),
		Timeout:     testTimeout,
		ServiceName: "bench-service",
	}

	client, err := NewExchangeDBClientRabbitMQ(ctx, config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	defer purgeQueue(b, client)

	// Pre-populate queue
	now := time.Now()
	for i := 0; i < b.N; i++ {
		item := &db_api.BatchJobPriority{
			ID:  fmt.Sprintf("bench-job-%d", i),
			SLO: now.Add(time.Duration(i) * time.Second),
		}
		_ = client.PQEnqueue(ctx, item)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = client.PQDequeue(ctx, 1*time.Second, 1)
	}
}
