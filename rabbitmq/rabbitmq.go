package rabbitmq

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

const delay = 3 // reconnect after delay seconds

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel(exchange *Exchange, queue *Queue, bind *QueueBind) (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel:  ch,
		exchange: exchange,
		queue:    queue,
		bind:     bind,
	}

	channel.exchangeDeclare()
	channel.queueDeclare()
	channel.queueBind()

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				debug("channel closed")
				_ = channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			debug("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(delay * time.Second)

				ch, err := c.Connection.Channel()
				if err == nil {
					debug("channel recreate success")
					channel.Channel = ch
					channel.exchangeDeclare()
					channel.queueDeclare()
					channel.queueBind()
					break
				}

				debugf("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				debug("connection closed")
				break
			}
			debugf("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(delay * time.Second)

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					debugf("reconnect success")
					break
				}

				debugf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

// Channel amqp.Channel wapper
type Channel struct {
	*amqp.Channel
	exchange *Exchange
	queue    *Queue
	bind     *QueueBind
	closed   int32
}

type Queue struct {
	QueueName  string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Args       amqp.Table
}

type QueueBind struct {
	Key  string
	Args amqp.Table
}

type Exchange struct {
	ExchangeName string
	Kind         string
	Durable      bool
	AutoDelete   bool
	Internal     bool
	NoWait       bool
	Args         amqp.Table
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Consume warp amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				debugf("consume failed, err: %v", err)
				time.Sleep(delay * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}

func (ch *Channel) exchangeDeclare() {
	err := ch.Channel.ExchangeDeclare(ch.exchange.ExchangeName, ch.exchange.Kind, ch.exchange.Durable, ch.exchange.AutoDelete, ch.exchange.Internal, ch.exchange.NoWait, ch.exchange.Args)
	if err != nil {
		log.Panic(err)
	}
}

func (ch *Channel) queueDeclare() {
	_, err := ch.Channel.QueueDeclare(ch.queue.QueueName, ch.queue.Durable, ch.queue.AutoDelete, ch.queue.Exclusive, ch.exchange.NoWait, ch.queue.Args)
	if err != nil {
		log.Panic(err)
	}
}

func (ch *Channel) queueBind() {
	err := ch.Channel.QueueBind(ch.queue.QueueName, ch.bind.Key, ch.exchange.ExchangeName, ch.exchange.NoWait, ch.bind.Args)
	if err != nil {
		log.Panic(err)
	}
}
