package amqp

import (
"context"
"encoding/json"
"os"
"testing"
"time"

"github.com/openjobspec/ojs-go-backend-common/core"
)

// --- Connection Tests ---

func TestConnection_NewConnection(t *testing.T) {
amqpURL := os.Getenv("AMQP_URL")
if amqpURL == "" {
amqpURL = "amqp://guest:guest@localhost:5672/"
}

conn, err := NewConnection(amqpURL)
if err != nil {
t.Skipf("cannot connect to AMQP: %v", err)
}
defer conn.Close()

if !conn.IsConnected() {
t.Error("connection should be established")
}
}

func TestConnection_EnsureQueue_Idempotent(t *testing.T) {
amqpURL := os.Getenv("AMQP_URL")
if amqpURL == "" {
amqpURL = "amqp://guest:guest@localhost:5672/"
}

conn, err := NewConnection(amqpURL)
if err != nil {
t.Skipf("cannot connect to AMQP: %v", err)
}
defer conn.Close()

queueName := "test-idempotent-q"

// First call
err1 := conn.EnsureQueue(queueName)
if err1 != nil {
t.Fatalf("first EnsureQueue failed: %v", err1)
}

// Second call should also succeed (idempotent)
err2 := conn.EnsureQueue(queueName)
if err2 != nil {
t.Fatalf("second EnsureQueue failed: %v", err2)
}
}

func TestConnection_Publish(t *testing.T) {
amqpURL := os.Getenv("AMQP_URL")
if amqpURL == "" {
amqpURL = "amqp://guest:guest@localhost:5672/"
}

conn, err := NewConnection(amqpURL)
if err != nil {
t.Skipf("cannot connect to AMQP: %v", err)
}
defer conn.Close()

queueName := "test-publish-q"
conn.EnsureQueue(queueName)

job := &core.Job{
ID:    "test-job-1",
Type:  "test.publish",
Queue: queueName,
Args:  json.RawMessage(`{"test":"data"}`),
}

ctx := context.Background()
err = conn.Publish(ctx, queueName, job)
if err != nil {
t.Fatalf("Publish failed: %v", err)
}
}

func TestConnection_PublishEvent(t *testing.T) {
amqpURL := os.Getenv("AMQP_URL")
if amqpURL == "" {
amqpURL = "amqp://guest:guest@localhost:5672/"
}

conn, err := NewConnection(amqpURL)
if err != nil {
t.Skipf("cannot connect to AMQP: %v", err)
}
defer conn.Close()

event := &core.JobEvent{
JobID:     "test-job-1",
Queue:     "default",
EventType: "job.started",
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}

ctx := context.Background()
err = conn.PublishEvent(ctx, event)
if err != nil {
t.Fatalf("PublishEvent failed: %v", err)
}
}

func TestConnection_Close(t *testing.T) {
amqpURL := os.Getenv("AMQP_URL")
if amqpURL == "" {
amqpURL = "amqp://guest:guest@localhost:5672/"
}

conn, err := NewConnection(amqpURL)
if err != nil {
t.Skipf("cannot connect to AMQP: %v", err)
}

if !conn.IsConnected() {
t.Fatal("connection should be established before close")
}

err = conn.Close()
if err != nil {
t.Fatalf("Close failed: %v", err)
}

// Give it a moment
time.Sleep(100 * time.Millisecond)

// After close, connection check might be unreliable due to async cleanup
// but we can verify no panic occurs
}

func TestConnection_MultipleQueues(t *testing.T) {
amqpURL := os.Getenv("AMQP_URL")
if amqpURL == "" {
amqpURL = "amqp://guest:guest@localhost:5672/"
}

conn, err := NewConnection(amqpURL)
if err != nil {
t.Skipf("cannot connect to AMQP: %v", err)
}
defer conn.Close()

queues := []string{"queue-a", "queue-b", "queue-c"}

for _, q := range queues {
err := conn.EnsureQueue(q)
if err != nil {
t.Fatalf("EnsureQueue(%s) failed: %v", q, err)
}
}

// Publish to each
for _, q := range queues {
job := &core.Job{
Type:  "test.multi",
Queue: q,
Args:  json.RawMessage(`{}`),
}
err := conn.Publish(context.Background(), q, job)
if err != nil {
t.Fatalf("Publish to %s failed: %v", q, err)
}
}
}
