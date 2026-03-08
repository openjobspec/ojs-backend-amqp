// Package events provides an in-process event broker for the AMQP backend,
// implementing core.EventPublisher and core.EventSubscriber for SSE/WebSocket support.
package events

import (
	"sync"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

// Broker is an in-process event broker implementing both EventPublisher and EventSubscriber.
// It also maintains an in-memory log of recent events for the GET /ojs/v1/events API.
type Broker struct {
	mu          sync.RWMutex
	subscribers map[string][]chan *core.JobEvent
	allSubs     []chan *core.JobEvent
	bufSize     int

	// Event log for query API
	eventLog []*core.JobEvent
	maxLog   int
}

// NewBroker creates a new in-process event broker.
func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string][]chan *core.JobEvent),
		allSubs:     make([]chan *core.JobEvent, 0),
		bufSize:     64,
		maxLog:      10000,
	}
}

// PublishJobEvent sends an event to all matching subscribers and stores it in the log.
func (b *Broker) PublishJobEvent(event *core.JobEvent) error {
	if event == nil {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Append to event log (ring buffer)
	if len(b.eventLog) >= b.maxLog {
		b.eventLog = b.eventLog[1:]
	}
	b.eventLog = append(b.eventLog, event)

	// Job-specific subscribers
	if subs, ok := b.subscribers["job:"+event.JobID]; ok {
		for _, ch := range subs {
			select {
			case ch <- event:
			default:
			}
		}
	}

	// Queue-specific subscribers
	if event.Queue != "" {
		if subs, ok := b.subscribers["queue:"+event.Queue]; ok {
			for _, ch := range subs {
				select {
				case ch <- event:
				default:
				}
			}
		}
	}

	// Global subscribers
	for _, ch := range b.allSubs {
		select {
		case ch <- event:
		default:
		}
	}

	return nil
}

// ListEvents returns stored events matching the given filters.
func (b *Broker) ListEvents(types []string, queues []string, limit int) []map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	typeSet := make(map[string]bool, len(types))
	for _, t := range types {
		typeSet[t] = true
	}
	queueSet := make(map[string]bool, len(queues))
	for _, q := range queues {
		queueSet[q] = true
	}

	var result []map[string]any
	// Iterate in reverse (newest first)
	for i := len(b.eventLog) - 1; i >= 0 && len(result) < limit; i-- {
		ev := b.eventLog[i]

		// Map internal event types to conformance-expected types
		eventType := ev.EventType
		if eventType == "job.state_changed" {
			if ev.From == "" {
				eventType = "job.enqueued"
			} else if ev.To != "" {
				eventType = "job." + ev.To
			}
		}

		if len(typeSet) > 0 && !typeSet[eventType] {
			continue
		}
		if len(queueSet) > 0 && !queueSet[ev.Queue] {
			continue
		}

		data := map[string]any{
			"job_id":      ev.JobID,
			"job_type":    ev.JobType,
			"queue":       ev.Queue,
			"from":        ev.From,
			"to":          ev.To,
			"attempt":     0,
			"duration_ms": 0,
		}

		result = append(result, map[string]any{
			"type":      eventType,
			"timestamp": ev.Timestamp,
			"data":      data,
		})
	}

	return result
}

// ResetLog clears the event log (used for conformance test isolation).
func (b *Broker) ResetLog() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.eventLog = nil
}

// SubscribeJob subscribes to events for a specific job ID.
func (b *Broker) SubscribeJob(jobID string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe("job:" + jobID)
}

// SubscribeQueue subscribes to events for a specific queue.
func (b *Broker) SubscribeQueue(queue string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe("queue:" + queue)
}

// SubscribeAll subscribes to all events.
func (b *Broker) SubscribeAll() (<-chan *core.JobEvent, func(), error) {
	ch := make(chan *core.JobEvent, b.bufSize)

	b.mu.Lock()
	b.allSubs = append(b.allSubs, ch)
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i, c := range b.allSubs {
			if c == ch {
				b.allSubs = append(b.allSubs[:i], b.allSubs[i+1:]...)
				break
			}
		}
		close(ch)
	}

	return ch, unsub, nil
}

// Close shuts down the broker.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, subs := range b.subscribers {
		for _, ch := range subs {
			close(ch)
		}
	}
	for _, ch := range b.allSubs {
		close(ch)
	}
	b.subscribers = make(map[string][]chan *core.JobEvent)
	b.allSubs = nil
	return nil
}

func (b *Broker) subscribe(key string) (<-chan *core.JobEvent, func(), error) {
	ch := make(chan *core.JobEvent, b.bufSize)

	b.mu.Lock()
	b.subscribers[key] = append(b.subscribers[key], ch)
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		subs := b.subscribers[key]
		for i, c := range subs {
			if c == ch {
				b.subscribers[key] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if len(b.subscribers[key]) == 0 {
			delete(b.subscribers, key)
		}
		close(ch)
	}

	return ch, unsub, nil
}
