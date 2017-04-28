// Copyright © 2017 Robbie Diaz <metaldrummer610@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package domain

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

// RabbitConfig contains all the common configurations needed to setup a RabbitMQ consumer/producer
type RabbitConfig struct {
	Url string
	Exchange struct {
		Name       string
		Kind       string
		Durable    bool
		AutoDelete bool
	}

	Queue struct {
		Name       string
		Durable    bool
		AutoDelete bool
		Exclusive  bool
	}

	Consumer struct {
		Tag       string
		AutoAck   bool
		Exclusive bool
	}

	Jitter struct {
		min int
		max int
	}
}

// RabbitClient wraps the common functionality of a RabbitMQ client (either a producer or a consumer)
type RabbitClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *RabbitConfig
	queue   amqp.Queue
}

type rabbitService struct {
	client *RabbitClient
}

type rabbitRoute struct {
	client *RabbitClient
}

func init() {
	serviceRegistry["rabbit"] = func(v *viper.Viper) Service {
		var config RabbitConfig
		if err := v.Unmarshal(&config); err != nil {
			Logger().Error("Unable to unmarshal rabbit configuration!", zap.Error(err))
		}

		client := &RabbitClient{config: &config}
		client.Connect()

		return &rabbitService{client: client}
	}

	routeRegistry["rabbit"] = func(v *viper.Viper) Route {
		var config RabbitConfig
		if err := v.Unmarshal(&config); err != nil {
			Logger().Error("Unable to unmarshal rabbit configuration!", zap.Error(err))
		}

		client := &RabbitClient{config: &config}
		client.Connect()

		return &rabbitRoute{client: client}
	}
}

func (r*rabbitService) Run(ctx context.Context, messages chan Message) (error) {
	Logger().Info("Rabbit listener started")

	deliveries, err := r.client.channel.Consume(
		r.client.queue.Name,
		r.client.config.Consumer.Tag,
		r.client.config.Consumer.AutoAck,
		r.client.config.Consumer.Exclusive,
		false,
		false,
		nil,
	)
	if err != nil {
		Logger().Error("Unable to consume a queue!", zap.Error(err))
		return err
	}

	for {
		Logger().Debug("Top of the loop")
		select {
		case <-ctx.Done():
			Logger().Debug("Context is finished. Breaking out", zap.Error(ctx.Err()))
			goto done
		case msg := <-deliveries:
			Logger().Debug("Got a message!",
				zap.Int("bodyLength", len(msg.Body)),
				zap.Uint64("deliveryTag", msg.DeliveryTag),
				zap.ByteString("body", msg.Body),
			)

			message := Message{}
			if err := proto.Unmarshal(msg.Body, &message); err != nil {
				Logger().Error("Failed to deserialize the protobuf message!",
					zap.Error(err),
				)

				break
			}

			Logger().Debug("Got message",
				zap.Stringer("message", &message),
			)
			messages <- message

			Logger().Debug("Message queued successfully!")

			if err := msg.Ack(false); err != nil {
				Logger().Error("Failed to ack message!", zap.Error(err))
			}
		}
		Logger().Debug("Bottom of the loop")
	}

done:
	r.client.Close()

	Logger().Info("Rabbit listener complete")
	return nil
}

func (r *rabbitRoute) Write(message Message) error {
	body, err := proto.Marshal(&message)

	if err != nil {
		Logger().Error("Failed to write message to NSQ topic!",
			zap.Error(err),
		)
		return err
	}

	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}

	return r.client.channel.Publish("", r.client.queue.Name, false, false, pub)
}

func (r *RabbitClient) Connect() error {
	var err error

	if r.conn, err = amqp.Dial(r.config.Url); err != nil {
		Logger().Error("Failed to connect to Rabbit!", zap.Error(err))
		return err
	}

	if r.channel, err = r.conn.Channel(); err != nil {
		Logger().Error("Failed to obtain a Rabbit channel!", zap.Error(err))
		return err
	}

	if err = r.channel.ExchangeDeclare(
		r.config.Exchange.Name,
		r.config.Exchange.Kind,
		r.config.Exchange.Durable,
		r.config.Exchange.AutoDelete,
		false,
		false,
		nil,
	); err != nil {
		Logger().Error("Unable to declare an exchange!", zap.Error(err))
		return err
	}

	if r.queue, err = r.channel.QueueDeclare(
		r.config.Queue.Name,
		r.config.Queue.Durable,
		r.config.Queue.AutoDelete,
		r.config.Queue.Exclusive,
		false,
		nil,
	);
		err != nil {
		Logger().Error("Failed to declare a Rabbit queue!", zap.Error(err))
		return err
	}

	return nil
}

func (r *RabbitClient) Close() {
	r.channel.Close()
	r.conn.Close()
}

func (r *rabbitRoute) Close() {
	r.client.Close()
}
