package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeDirect = "ojs.direct"
	exchangeDLX    = "ojs.dlx"
	exchangeRetry  = "ojs.retry"
	exchangeEvents = "ojs.events"

	reconnectDelay = 3 * time.Second
	publishTimeout = 5 * time.Second
)

// Connection manages the AMQP connection and channel with automatic reconnection.
type Connection struct {
	url  string
	conn *amqp091.Connection
	ch   *amqp091.Channel

	mu     sync.RWMutex
	closed bool
	done   chan struct{}

	// Track declared queues to avoid re-declaring on every publish
	declaredQueues map[string]bool
	declaredMu     sync.Mutex
}

// NewConnection establishes an AMQP connection, declares exchanges, and returns
// a Connection manager. If the initial connection fails, an error is returned.
func NewConnection(amqpURL string) (*Connection, error) {
	c := &Connection{
		url:            amqpURL,
		done:           make(chan struct{}),
		declaredQueues: make(map[string]bool),
	}

	if err := c.connect(); err != nil {
		return nil, fmt.Errorf("initial AMQP connection: %w", err)
	}

	if err := c.declareExchanges(); err != nil {
		c.conn.Close()
		return nil, fmt.Errorf("declare exchanges: %w", err)
	}

	// Start reconnection monitor
	go c.reconnectLoop()

	slog.Info("AMQP connection established", "url", amqpURL)
	return c, nil
}

func (c *Connection) connect() error {
	conn, err := amqp091.Dial(c.url)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.ch = ch
	c.mu.Unlock()

	// Reset declared queues on reconnect
	c.declaredMu.Lock()
	c.declaredQueues = make(map[string]bool)
	c.declaredMu.Unlock()

	return nil
}

func (c *Connection) declareExchanges() error {
	c.mu.RLock()
	ch := c.ch
	c.mu.RUnlock()

	exchanges := []struct {
		name string
		kind string
	}{
		{exchangeDirect, "direct"},
		{exchangeDLX, "direct"},
		{exchangeRetry, "direct"},
		{exchangeEvents, "fanout"},
	}

	for _, ex := range exchanges {
		err := ch.ExchangeDeclare(ex.name, ex.kind, true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("declare exchange %s: %w", ex.name, err)
		}
	}

	return nil
}

// EnsureQueue declares the AMQP queue, dead letter queue, and bindings for an OJS queue.
// Idempotent — skips if already declared in this connection lifecycle.
func (c *Connection) EnsureQueue(queueName string) error {
	c.declaredMu.Lock()
	if c.declaredQueues[queueName] {
		c.declaredMu.Unlock()
		return nil
	}
	c.declaredMu.Unlock()

	c.mu.RLock()
	ch := c.ch
	c.mu.RUnlock()

	if ch == nil {
		return fmt.Errorf("AMQP channel not available")
	}

	amqpQueue := "ojs.queue." + queueName
	dlqQueue := "ojs.queue." + queueName + ".dlq"

	// Declare primary queue with dead letter exchange
	_, err := ch.QueueDeclare(amqpQueue, true, false, false, false, amqp091.Table{
		"x-dead-letter-exchange":    exchangeDLX,
		"x-dead-letter-routing-key": dlqQueue,
	})
	if err != nil {
		return fmt.Errorf("declare queue %s: %w", amqpQueue, err)
	}

	// Bind primary queue to direct exchange
	if err := ch.QueueBind(amqpQueue, queueName, exchangeDirect, false, nil); err != nil {
		return fmt.Errorf("bind queue %s: %w", amqpQueue, err)
	}

	// Declare dead letter queue
	_, err = ch.QueueDeclare(dlqQueue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare DLQ %s: %w", dlqQueue, err)
	}

	// Bind DLQ to DLX exchange
	if err := ch.QueueBind(dlqQueue, dlqQueue, exchangeDLX, false, nil); err != nil {
		return fmt.Errorf("bind DLQ %s: %w", dlqQueue, err)
	}

	c.declaredMu.Lock()
	c.declaredQueues[queueName] = true
	c.declaredMu.Unlock()

	return nil
}

// Publish sends a job as a JSON message to the appropriate queue via the direct exchange.
func (c *Connection) Publish(ctx context.Context, queueName string, job interface{}) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	c.mu.RLock()
	ch := c.ch
	c.mu.RUnlock()

	if ch == nil {
		return fmt.Errorf("AMQP channel not available")
	}

	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	return ch.PublishWithContext(pubCtx, exchangeDirect, queueName, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
}

// PublishEvent sends a lifecycle event to the fanout exchange.
func (c *Connection) PublishEvent(ctx context.Context, event interface{}) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	c.mu.RLock()
	ch := c.ch
	c.mu.RUnlock()

	if ch == nil {
		return fmt.Errorf("AMQP channel not available")
	}

	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	return ch.PublishWithContext(pubCtx, exchangeEvents, "", false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

// IsConnected returns true if the AMQP connection is alive.
func (c *Connection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && !c.conn.IsClosed()
}

// Close shuts down the AMQP connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	close(c.done)

	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// reconnectLoop monitors the connection and reconnects on failure.
func (c *Connection) reconnectLoop() {
	for {
		select {
		case <-c.done:
			return
		default:
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			time.Sleep(reconnectDelay)
			continue
		}

		// Block until connection closes
		reason, ok := <-conn.NotifyClose(make(chan *amqp091.Error))
		if !ok {
			// Channel closed cleanly (intentional shutdown)
			return
		}

		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		slog.Warn("AMQP connection lost, reconnecting", "reason", reason)

		for {
			select {
			case <-c.done:
				return
			default:
			}

			time.Sleep(reconnectDelay)

			if err := c.connect(); err != nil {
				slog.Error("AMQP reconnect failed", "error", err)
				continue
			}

			if err := c.declareExchanges(); err != nil {
				slog.Error("AMQP re-declare exchanges failed", "error", err)
				continue
			}

			slog.Info("AMQP reconnected successfully")
			break
		}
	}
}
