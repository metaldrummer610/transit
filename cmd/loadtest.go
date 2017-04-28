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

package cmd

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/metaldrummer610/transit/domain"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"time"
)

// loadtestCmd represents the loadtest command
var loadtestCmd = &cobra.Command{
	Use:   "loadtest",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		domain.ConfigureLogger(viper.GetViper())

		producerFlag := cmd.Flag("producer")
		consumerFlag := cmd.Flag("consumer")
		destinationFlag := cmd.Flag("destination")

		if producerFlag.Changed {
			producerConfig := viper.Sub(fmt.Sprintf("services.%s", producerFlag.Value))
			go setupProducer(producerConfig, destinationFlag.Value.String())
		}

		if consumerFlag.Changed {
			consumerConfig := viper.Sub(fmt.Sprintf("routes.%s", consumerFlag.Value))
			go setupConsumer(consumerConfig)
		}

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		<-c
	},
}

func setupProducer(v *viper.Viper, destination string) {
	switch v.GetString("protocol") {
	case "rabbit":
		createRabbitProducer(v, destination)
	}
}

func setupConsumer(v *viper.Viper) {
	switch v.GetString("protocol") {
	case "http":
		createWebConsumer(v)
	}
}

func createRabbitProducer(v *viper.Viper, destination string) {
	var config domain.RabbitConfig
	if err := v.Unmarshal(&config); err != nil {
		domain.Logger().Error("Unable to unmarshal rabbit configuration!", zap.Error(err))
	}

	var conn *amqp.Connection
	var channel *amqp.Channel
	var queue amqp.Queue

	var err error

	if conn, err = amqp.Dial(config.Url); err != nil {
		domain.Logger().Error("Failed to connect to Rabbit!", zap.Error(err))
		return
	}

	if channel, err = conn.Channel(); err != nil {
		domain.Logger().Error("Failed to obtain a Rabbit channel!", zap.Error(err))
		return
	}

	if err = channel.ExchangeDeclare(
		config.Exchange.Name,
		config.Exchange.Kind,
		config.Exchange.Durable,
		config.Exchange.AutoDelete,
		false,
		false,
		nil,
	); err != nil {
		domain.Logger().Error("Unable to declare an exchange!", zap.Error(err))
		return
	}

	if queue, err = channel.QueueDeclare(
		config.Queue.Name,
		config.Queue.Durable,
		config.Queue.AutoDelete,
		config.Queue.Exclusive,
		false,
		nil,
	);
		err != nil {
		domain.Logger().Error("Failed to declare a Rabbit queue!", zap.Error(err))
		return
	}

	for {
		expected := &domain.Message{
			Body:        []byte(`Hello World!`),
			Destination: destination,
		}

		body, _ := proto.Marshal(expected)

		channel.Publish(
			"",
			queue.Name,
			false,
			false,
			amqp.Publishing{
				Body: body,
			},
		)
	}
}

func createWebConsumer(v *viper.Viper) {
	endpoint, _ := url.Parse(v.GetString("endpoint"))

	_, port, _ := net.SplitHostPort(endpoint.Host)

	domain.Logger().Info("Listening on", zap.String("port", port))

	server := &http.Server{Addr: fmt.Sprintf(":%s", port)}
	http.HandleFunc("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		ioutil.ReadAll(r.Body)

		time.Sleep(time.Duration(int(rand.Intn(1)) * int(time.Second)))

		rand.Seed(time.Now().UnixNano())
		value := rand.Intn(100) + 1
		if value <= 25 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))

	domain.Logger().Info("Starting http server")
	domain.Logger().Info("Listen and serve finished!", zap.Error(server.ListenAndServe()))
}

func init() {
	RootCmd.AddCommand(loadtestCmd)
	loadtestCmd.Flags().StringP("producer", "p", "", "Name of the service to produce data for")
	loadtestCmd.Flags().StringP("consumer", "c", "", "Name of the route to consume data from")
	loadtestCmd.Flags().StringP("destination", "d", "", "Destination for the message")
}
