package amqp

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

// Client manages the RabbitMQ connection and channel
type Client struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.RWMutex
	url     string
}

// NewClient creates a new AMQP client
func NewClient(url string) (*Client, error) {
	client := &Client{
		url: url,
	}

	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("failed to create AMQP client: %w", err)
	}

	return client, nil
}

// connect establishes connection and channel
func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	c.conn = conn
	c.channel = ch

	// Set up connection close notification
	go c.handleConnectionClose()

	log.Info("AMQP client connected successfully")
	return nil
}

// handleConnectionClose listens for connection close events
func (c *Client) handleConnectionClose() {
	closeErr := make(chan *amqp.Error)
	c.conn.NotifyClose(closeErr)

	err := <-closeErr
	if err != nil {
		log.Errorf("AMQP connection closed: %v", err)
	}
}

// Channel returns the current channel (use with caution, prefer Publisher/Consumer)
func (c *Client) Channel() *amqp.Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.channel
}

// Close closes the channel and connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error

	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close channel: %w", err))
		}
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	log.Info("AMQP client closed successfully")
	return nil
}
