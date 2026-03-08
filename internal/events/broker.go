// Package events provides an in-process event broker for the AMQP backend,
// implementing core.EventPublisher and core.EventSubscriber for SSE/WebSocket support.
package events

import (
	"sync"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

// Broker is an in-process event broker implementing both EventPublisher and EventSubscriber.
type Broker struct {
	mu          sync.RWMutex
	subscribers map[string][]chan *core.JobEvent
	allSubs     []chan *core.JobEvent
	bufSize     int
}

// NewBroker creates a new in-process event broker.
func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string][]chan *core.JobEvent),
		allSubs:     make([]chan *core.JobEvent, 0),
		bufSize:     64,
	}
}

// PublishJobEvent sends an event to all matching subscribers.
func (b *Broker) PublishJobEvent(event *core.JobEvent) error {
	if event == nil {
		return nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

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
