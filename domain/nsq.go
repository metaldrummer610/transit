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
	"github.com/bitly/go-nsq"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type nsqService struct {
	consumer   *nsq.Consumer
	nsqlookupd string
}

type nsqRoute struct {
	producer *nsq.Producer
	topic    string
}

func init() {
	serviceRegistry["nsq"] = func(v *viper.Viper) Service {
		config := nsq.NewConfig()
		config.BackoffStrategy = &nsq.FullJitterStrategy{}
		config.MaxAttempts = uint16(v.GetInt("jitter.attempts"))

		consumer, err := nsq.NewConsumer(v.GetString("topic"), v.GetString("channel"), config)
		if err != nil {
			Logger().Fatal("Failed to configure NSQ consumer!",
				zap.Error(err),
			)
		}

		return &nsqService{
			consumer:   consumer,
			nsqlookupd: v.GetString("nsqlookupd"),
		}
	}

	routeRegistry["nsq"] = func(v *viper.Viper) Route {
		config := nsq.NewConfig()

		producer, err := nsq.NewProducer(v.GetString("hostname"), config)
		if err != nil {
			Logger().Fatal("Failed to configure NSQ producer!",
				zap.Error(err),
			)
		}

		return &nsqRoute{
			producer: producer,
			topic:    v.GetString("topic"),
		}
	}
}

func (n*nsqService) Run(ctx context.Context, messages chan Message) error {
	n.consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		msg := Message{}
		if err := proto.Unmarshal(message.Body, &msg); err != nil {
			Logger().Error("Failed to deserialize the protobuf message!",
				zap.Error(err),
			)

			return err
		}

		messages <- msg
		return nil
	}))

	if err := n.consumer.ConnectToNSQLookupd(n.nsqlookupd); err != nil {
		Logger().Fatal("Unable to connect to NSQ using lookup!",
			zap.Error(err),
		)
	}

	for {
		select {
		case <-ctx.Done():
			n.consumer.Stop()
			return ctx.Err()
		}
	}
}

func (n*nsqRoute) Write(message Message) error {
	body, err := proto.Marshal(&message)

	if err != nil {
		Logger().Error("Failed to write message to NSQ topic!",
			zap.Error(err),
		)
		return err
	}

	if err := n.producer.Publish(n.topic, body); err != nil {
		Logger().Error("Failed to write message to NSQ topic!",
			zap.Error(err),
		)

		return err
	}

	return nil
}

func (n*nsqRoute) Close() {
	n.producer.Stop()
}