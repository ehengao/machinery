package brokers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	// "github.com/satori/go.uuid"
	"sync"
	"time"

	"github.com/ehengao/machinery/v2/common"
	"github.com/ehengao/machinery/v2/config"
	_log "github.com/ehengao/machinery/v2/log"
	"github.com/ehengao/machinery/v2/tasks"
	"github.com/mitchellh/mapstructure"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

var log = _log.GetLogger()

// AMQPBroker represents an AMQP broker
type AMQPBroker struct {
	Broker
	common.AMQPConnector
	processingWG sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
}

// NewAMQPBroker creates new AMQPBroker instance
func NewAMQPBroker(cnf *config.Config) Interface {
	return &AMQPBroker{Broker: New(cnf), AMQPConnector: common.AMQPConnector{}}
}

// StartConsuming enters a loop and waits for incoming messages
func (b *AMQPBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)

	conn, channel, queue, _, amqpCloseChan, err := b.Connect(
		b.cnf.Broker,
		b.cnf.TLSConfig,
		b.cnf.AMQP.Exchange,                     // exchange name
		b.cnf.AMQP.ExchangeType,                 // exchange type
		b.cnf.DefaultQueue,                      // queue name
		true,                                    // queue durable
		false,                                   // queue delete when unused
		b.cnf.AMQP.BindingKey,                   // queue binding key
		nil,                                     // exchange declare args
		nil,                                     // queue declare args
		amqp.Table(b.cnf.AMQP.QueueBindingArgs), // queue binding args
	)
	if err != nil {
		b.retryFunc(b.retryStopChan)
		return b.retry, err
	}
	defer b.Close(channel, conn)

	if err = channel.Qos(
		b.cnf.AMQP.PrefetchCount,
		0,     // prefetch size
		false, // global
	); err != nil {
		return b.retry, fmt.Errorf("Channel qos error: %s", err)
	}

	deliveries, err := channel.Consume(
		queue.Name,  // queue
		consumerTag, // consumer tag
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return b.retry, fmt.Errorf("Queue consume error: %s", err)
	}

	defer log.Sync()
	log.Info("[*] Waiting for messages. To exit press CTRL+C")

	if err := b.consume(deliveries, concurrency, taskProcessor, amqpCloseChan); err != nil {
		return b.retry, err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *AMQPBroker) StopConsuming() {
	b.stopConsuming()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *AMQPBroker) Publish(signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	log.Info("sending task", zap.String("signature.Name", signature.Name))
	AdjustRoutingKey(b, signature)

	msgProtocol := b.GetConfig().CeleryMessage
	var msg []byte
	var err error
	if msgProtocol == 0 {
		msg, err = json.Marshal(signature)
	} else if msgProtocol == 2 {
		embed := new(tasks.Embed)
		embed.UnmarshalSignature(signature)
		body := &tasks.CeleryMessageBody{
			Args:   signature.Args,
			Kwargs: signature.Kwargs,
			Embed:  *embed,
		}
		if signature.ChordCallback != nil {
			body.Embed.Chord = new(tasks.CeleryCallback)
			body.Embed.Chord.UnmarshalSignature(signature.ChordCallback)
		}
		msg, err = json.Marshal([]interface{}{
			body.Args,
			body.Kwargs,
			body.Embed,
		})
	}
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			delayMs := int64(signature.ETA.Sub(now) / time.Millisecond)

			return b.delay(signature, delayMs)
		}
	}
	exchange := b.cnf.AMQP.Exchange
	if ex, ok := b.cnf.AMQP.TaskRoutes[signature.Name]; ok {
		exchange = ex
	}
	signature.RoutingKey = exchange
	conn, channel, _, confirmsChan, _, err := b.Connect(
		b.cnf.Broker,
		b.cnf.TLSConfig,
		// b.cnf.AMQP.Exchange,     // exchange name
		exchange,
		b.cnf.AMQP.ExchangeType,                 // exchange type
		signature.RoutingKey,                    // queue name
		true,                                    // queue durable
		false,                                   // queue delete when unused
		b.cnf.AMQP.BindingKey,                   // queue binding key
		nil,                                     // exchange declare args
		nil,                                     // queue declare args
		amqp.Table(b.cnf.AMQP.QueueBindingArgs), // queue binding args
	)
	if err != nil {
		return err
	}
	defer b.Close(channel, conn)

	if err := channel.Publish(
		// b.cnf.AMQP.Exchange,  // exchange name
		exchange,
		signature.RoutingKey, // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			Headers:       amqp.Table(signature.Headers),
			ContentType:   "application/json",
			Body:          msg,
			DeliveryMode:  amqp.Persistent,
			CorrelationId: signature.UUID,
			ReplyTo:       signature.ReplyTo,
		},
	); err != nil {
		return err
	}

	confirmed := <-confirmsChan

	if confirmed.Ack {
		return nil
	}

	return fmt.Errorf("Failed delivery of delivery tag: %v", confirmed.DeliveryTag)
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *AMQPBroker) consume(deliveries <-chan amqp.Delivery, concurrency int, taskProcessor TaskProcessor, amqpCloseChan <-chan *amqp.Error) error {
	pool := make(chan struct{}, concurrency)

	// initializeG worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)

	for {
		select {
		case amqpErr := <-amqpCloseChan:
			return amqpErr
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor, pool); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.stopChan:
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *AMQPBroker) consumeOne(delivery amqp.Delivery, taskProcessor TaskProcessor, pool chan struct{}) error {
	msgProtocol := b.GetConfig().CeleryMessage
	if len(delivery.Body) == 0 {
		delivery.Nack(true, false)                     // multiple, requeue
		return errors.New("Received an empty message") // RabbitMQ down?
	}

	var multiple, requeue = false, false
	signature := new(tasks.Signature)
	signature.ReplyTo = delivery.ReplyTo
	signature.Priority = delivery.Priority
	signature.ContentType = delivery.ContentType
	signature.ContentEncoding = delivery.ContentEncoding
	decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
	decoder.UseNumber()
	// Unmarshal message body into signature struct
	defer log.Sync()
	if msgProtocol == 0 {
		if err := decoder.Decode(signature); err != nil {
			delivery.Nack(multiple, requeue)
			return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, err)
		}
		if !b.IsTaskRegistered(signature.Name) {
			if !delivery.Redelivered {
				requeue = true
				log.Error("Task not registered with this worker. Requeing message", zap.ByteString("delivery.Body", delivery.Body))
			}
			delivery.Nack(multiple, requeue)
			return nil
		}
		log.Debug("Received new message", zap.ByteString("delivery.Body", delivery.Body))
		err := taskProcessor.Process(signature)
		delivery.Ack(multiple)
		return err
	} else if msgProtocol == 2 {
		headers := new(tasks.CeleryMessageHeaders)
		err := mapstructure.Decode(delivery.Headers, headers)
		if err != nil {
			delivery.Nack(multiple, requeue)
			return NewErrCouldNotUnmarshaTaskSignature([]byte(fmt.Sprintf("Headers: %s", delivery.Headers)), err)
		}
		if !b.IsTaskRegistered(headers.Task) {
			if !delivery.Redelivered {
				requeue = true
				log.Error("Task not registered with this worker. Requeing message", zap.ByteString("delivery.Body", delivery.Body))
			}
			delivery.Nack(multiple, requeue)
			return nil
		}
		log.Debug("Received new celery message", zap.String("delivery.RoutingKey", delivery.RoutingKey), zap.String("headers.Task", headers.Task), zap.String("headers.Id", headers.Id), zap.ByteString("delivery.Body", delivery.Body))
		signature.Name = headers.Task
		signature.UUID = headers.Id
		signature.RoutingKey = delivery.RoutingKey
		signature.ETA = headers.ETA
		signature.GroupUUID = headers.Group
		signature.RetryCount = headers.Retries
		signature.RetryTimeout = headers.Timelimit[1]
		if headers.BatchSize > 1 {
			celeryMsgBatch := new(tasks.CeleryMessageBodyBatch)
			if err := decoder.Decode(celeryMsgBatch); err != nil {
				delivery.Nack(multiple, requeue)
				return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, err)
			}
			if len(celeryMsgBatch.Args) != headers.BatchSize {
				delivery.Nack(multiple, requeue)
				return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, fmt.Errorf("Batch size is in consistnt with argument size!"))
			}
			ss := []*tasks.Signature{}

			for i := 0; i < headers.BatchSize; i++ {
				args := celeryMsgBatch.Args[i].Args
				s := new(tasks.Signature)
				s.Args = args
				s.Name = headers.Task
				s.UUID = headers.Id
				s.RoutingKey = delivery.RoutingKey
				s.ETA = headers.ETA
				s.GroupUUID = headers.Group
				s.RetryCount = headers.Retries
				s.RetryTimeout = headers.Timelimit[1]
				ss = append(ss, s)
			}
			if celeryMsgBatch.Embed.Chord != nil {
				s, e := tasks.CreateSignatureFromChord(celeryMsgBatch.Embed.Chord, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "batch_size")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.GroupTaskCount = celeryMsgBatch.Embed.Chord.ChordSize
				signature.ChordCallback = s
			}
			if celeryMsgBatch.Embed.Callbacks != nil && len(celeryMsgBatch.Embed.Callbacks) > 0 {
				s, e := tasks.CreateSignatureFromCallbacks(celeryMsgBatch.Embed.Callbacks, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "batch_size")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.OnSuccess = append(signature.OnSuccess, s)
			}
			if celeryMsgBatch.Embed.Chain != nil && len(celeryMsgBatch.Embed.Chain) > 0 {
				s, e := tasks.CreateSignatureFromChain(celeryMsgBatch.Embed.Chain, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "batch_size")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.OnSuccess = append(signature.OnSuccess, s)
			}
			if celeryMsgBatch.Embed.Errbacks != nil && len(celeryMsgBatch.Embed.Errbacks) > 0 {
				s, e := tasks.CreateSignatureFromCallbacks(celeryMsgBatch.Embed.Errbacks, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "batch_size")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.OnError = append(signature.OnError, s)
			}
			err = taskProcessor.ProcessBatch(signature, ss, pool)
			delivery.Ack(multiple)
			return err
		} else {
			celeryMsg := new(tasks.CeleryMessageBody)
			if err := decoder.Decode(celeryMsg); err != nil {
				delivery.Nack(multiple, requeue)
				return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, err)
			}
			signature.Args = celeryMsg.Args
			if celeryMsg.Embed.Chord != nil {
				s, e := tasks.CreateSignatureFromChord(celeryMsg.Embed.Chord, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.GroupTaskCount = celeryMsg.Embed.Chord.ChordSize
				signature.ChordCallback = s
			}
			if celeryMsg.Embed.Callbacks != nil && len(celeryMsg.Embed.Callbacks) > 0 {
				s, e := tasks.CreateSignatureFromCallbacks(celeryMsg.Embed.Callbacks, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.OnSuccess = append(signature.OnSuccess, s)
			}
			if celeryMsg.Embed.Chain != nil && len(celeryMsg.Embed.Chain) > 0 {
				s, e := tasks.CreateSignatureFromChain(celeryMsg.Embed.Chain, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.OnSuccess = append(signature.OnSuccess, s)
			}
			if celeryMsg.Embed.Errbacks != nil && len(celeryMsg.Embed.Errbacks) > 0 {
				s, e := tasks.CreateSignatureFromCallbacks(celeryMsg.Embed.Errbacks, delivery, headers.Group, tasks.Headers(delivery.Headers), signature.ReplyTo, "")
				if e != nil {
					delivery.Nack(multiple, requeue)
					return NewErrCouldNotUnmarshaTaskSignature(delivery.Body, e)
				}
				signature.OnError = append(signature.OnError, s)
			}
			err = taskProcessor.Process(signature)
			delivery.Ack(multiple)
			return err
		}
	}
	return nil
}

// delay a task by delayDuration miliseconds, the way it works is a new queue
// is created without any consumers, the message is then published to this queue
// with appropriate ttl expiration headers, after the expiration, it is sent to
// the proper queue with consumers
func (b *AMQPBroker) delay(signature *tasks.Signature, delayMs int64) error {
	if delayMs <= 0 {
		return errors.New("Cannot delay task by 0ms")
	}

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// It's necessary to redeclare the queue each time (to zero its TTL timer).
	queueName := fmt.Sprintf(
		"delay.%d.%s.%s",
		delayMs, // delay duration in mileseconds
		b.cnf.AMQP.Exchange,
		b.cnf.AMQP.BindingKey, // routing key
	)
	declareQueueArgs := amqp.Table{
		// Exchange where to send messages after TTL expiration.
		"x-dead-letter-exchange": b.cnf.AMQP.Exchange,
		// Routing key which use when resending expired messages.
		"x-dead-letter-routing-key": b.cnf.AMQP.BindingKey,
		// Time in milliseconds
		// after that message will expire and be sent to destination.
		"x-message-ttl": delayMs,
		// Time after that the queue will be deleted.
		"x-expires": delayMs * 2,
	}
	conn, channel, _, _, _, err := b.Connect(
		b.cnf.Broker,
		b.cnf.TLSConfig,
		b.cnf.AMQP.Exchange,                     // exchange name
		b.cnf.AMQP.ExchangeType,                 // exchange type
		queueName,                               // queue name
		true,                                    // queue durable
		false,                                   // queue delete when unused
		queueName,                               // queue binding key
		nil,                                     // exchange declare args
		declareQueueArgs,                        // queue declare args
		amqp.Table(b.cnf.AMQP.QueueBindingArgs), // queue binding args
	)
	if err != nil {
		return err
	}
	defer b.Close(channel, conn)

	if err := channel.Publish(
		b.cnf.AMQP.Exchange, // exchange
		queueName,           // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			Headers:      amqp.Table(signature.Headers),
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent,
		},
	); err != nil {
		return err
	}

	return nil
}
