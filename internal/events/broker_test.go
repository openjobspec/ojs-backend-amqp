package events

import (
"testing"
"time"

"github.com/openjobspec/ojs-go-backend-common/core"
)

// --- Event Broker Tests ---

func TestBroker_NewBroker(t *testing.T) {
b := NewBroker()
if b == nil {
t.Fatal("expected broker to be created")
}
if b.bufSize != 64 {
t.Errorf("expected bufSize 64, got %d", b.bufSize)
}
}

func TestBroker_SubscribeJob(t *testing.T) {
b := NewBroker()
defer b.Close()

jobID := "test-job-1"
ch, unsub, err := b.SubscribeJob(jobID)
if err != nil {
t.Fatalf("SubscribeJob failed: %v", err)
}
defer unsub()

if ch == nil {
t.Fatal("expected non-nil channel")
}
}

func TestBroker_SubscribeQueue(t *testing.T) {
b := NewBroker()
defer b.Close()

queue := "test-queue"
ch, unsub, err := b.SubscribeQueue(queue)
if err != nil {
t.Fatalf("SubscribeQueue failed: %v", err)
}
defer unsub()

if ch == nil {
t.Fatal("expected non-nil channel")
}
}

func TestBroker_SubscribeAll(t *testing.T) {
b := NewBroker()
defer b.Close()

ch, unsub, err := b.SubscribeAll()
if err != nil {
t.Fatalf("SubscribeAll failed: %v", err)
}
defer unsub()

if ch == nil {
t.Fatal("expected non-nil channel")
}
}

func TestBroker_PublishJobEvent(t *testing.T) {
b := NewBroker()
defer b.Close()

jobID := "test-job-123"
queue := "test-queue"

// Subscribe to job
ch, unsub, _ := b.SubscribeJob(jobID)
defer unsub()

// Publish event
event := &core.JobEvent{
JobID:     jobID,
Queue:     queue,
EventType: "job.started",
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}

err := b.PublishJobEvent(event)
if err != nil {
t.Fatalf("PublishJobEvent failed: %v", err)
}

// Receive event
select {
case received := <-ch:
if received.JobID != jobID {
t.Errorf("expected JobID %q, got %q", jobID, received.JobID)
}
if received.EventType != "job.started" {
t.Errorf("expected type 'job.started', got %q", received.EventType)
}
case <-time.After(1 * time.Second):
t.Fatal("timeout waiting for event")
}
}

func TestBroker_PublishJobEvent_MultipleSubscribers(t *testing.T) {
b := NewBroker()
defer b.Close()

jobID := "test-job-456"
queue := "test-queue"

// Multiple subscribers
ch1, unsub1, _ := b.SubscribeJob(jobID)
defer unsub1()
ch2, unsub2, _ := b.SubscribeJob(jobID)
defer unsub2()

event := &core.JobEvent{
JobID:     jobID,
Queue:     queue,
EventType: "job.completed",
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}

b.PublishJobEvent(event)

// Both should receive
for _, ch := range [](<-chan *core.JobEvent){ch1, ch2} {
select {
case received := <-ch:
if received.JobID != jobID {
t.Errorf("expected JobID %q, got %q", jobID, received.JobID)
}
case <-time.After(1 * time.Second):
t.Fatal("timeout waiting for event")
}
}
}

func TestBroker_PublishQueueEvent(t *testing.T) {
b := NewBroker()
defer b.Close()

queue := "test-queue"

// Subscribe to queue
ch, unsub, _ := b.SubscribeQueue(queue)
defer unsub()

// Publish event
event := &core.JobEvent{
JobID:     "job-1",
Queue:     queue,
EventType: "job.pushed",
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}

b.PublishJobEvent(event)

// Should receive
select {
case received := <-ch:
if received.Queue != queue {
t.Errorf("expected queue %q, got %q", queue, received.Queue)
}
case <-time.After(1 * time.Second):
t.Fatal("timeout waiting for event")
}
}

func TestBroker_PublishGlobalEvent(t *testing.T) {
b := NewBroker()
defer b.Close()

// Subscribe globally
ch, unsub, _ := b.SubscribeAll()
defer unsub()

event := &core.JobEvent{
JobID:     "job-global",
Queue:     "any-queue",
EventType: "job.started",
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}

b.PublishJobEvent(event)

// Should receive
select {
case received := <-ch:
if received.JobID != "job-global" {
t.Errorf("expected JobID 'job-global', got %q", received.JobID)
}
case <-time.After(1 * time.Second):
t.Fatal("timeout waiting for event")
}
}

func TestBroker_SubscribeUnsubscribe(t *testing.T) {
b := NewBroker()
defer b.Close()

jobID := "test-job-unsub"

ch, unsub, _ := b.SubscribeJob(jobID)

// Unsubscribe
unsub()

// Channel should be closed
select {
case _, ok := <-ch:
if ok {
t.Fatal("expected channel to be closed")
}
case <-time.After(100 * time.Millisecond):
t.Error("timeout waiting for channel close")
}
}

func TestBroker_PublishNilEvent(t *testing.T) {
b := NewBroker()
defer b.Close()

err := b.PublishJobEvent(nil)
if err != nil {
t.Errorf("expected no error for nil event, got %v", err)
}
}

func TestBroker_Close(t *testing.T) {
b := NewBroker()

// Subscribe before close
ch1, _, _ := b.SubscribeJob("job-1")
ch2, _, _ := b.SubscribeAll()

// Close broker
b.Close()

// Both channels should be closed
for _, ch := range [](<-chan *core.JobEvent){ch1, ch2} {
select {
case _, ok := <-ch:
if ok {
t.Fatal("expected channel to be closed")
}
case <-time.After(100 * time.Millisecond):
t.Error("timeout waiting for channel close")
}
}
}

func TestBroker_MultipleEventTypes(t *testing.T) {
b := NewBroker()
defer b.Close()

jobID := "test-job-multi-events"

ch, unsub, _ := b.SubscribeJob(jobID)
defer unsub()

// Publish multiple events
eventTypes := []string{"job.queued", "job.started", "job.completed"}
for _, eventType := range eventTypes {
event := &core.JobEvent{
JobID:     jobID,
Queue:     "test-queue",
EventType: eventType,
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}
b.PublishJobEvent(event)
}

// Receive all events
received := make([]string, 0)
for i := 0; i < 3; i++ {
select {
case event := <-ch:
received = append(received, event.EventType)
case <-time.After(1 * time.Second):
t.Fatalf("timeout waiting for event %d", i+1)
}
}

if len(received) != 3 {
t.Errorf("expected 3 events, got %d", len(received))
}
}

func TestBroker_NonBlockingSend(t *testing.T) {
b := NewBroker()
defer b.Close()

jobID := "test-job-buffered"

_, unsub, _ := b.SubscribeJob(jobID)
defer unsub()

// Publish 64 events (fills buffer)
for i := 0; i < 64; i++ {
event := &core.JobEvent{
JobID:     jobID,
Queue:     "test-queue",
EventType: "job.event",
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}
err := b.PublishJobEvent(event)
if err != nil {
t.Fatalf("PublishJobEvent failed: %v", err)
}
}

// Next publish should not block (uses default case)
event := &core.JobEvent{
JobID:     jobID,
Queue:     "test-queue",
EventType: "job.overflow",
Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
}

done := make(chan bool, 1)
go func() {
b.PublishJobEvent(event)
done <- true
}()

select {
case <-done:
// Success - didn't block
case <-time.After(500 * time.Millisecond):
t.Error("PublishJobEvent blocked (should have default case)")
}
}
