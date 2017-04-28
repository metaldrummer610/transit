package pkg_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/metaldrummer610/transit/domain"
	"github.com/metaldrummer610/transit/pkg"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"
)

const amqpURI = "amqp://guest:guest@127.0.0.1:5672"

var exchange string
var queueName string

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func randomName(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

var _ = Describe("Router", func() {
	var ctx context.Context
	var cancel context.CancelFunc

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())

		queueName = randomName(8)
		exchange = randomName(8)

		viper.SetConfigType("yaml")
		var config = []byte(fmt.Sprintf(`version: v1
logging:
  development: true
services:
  rabbit:
    protocol: rabbit
    url: amqp://guest:guest@127.0.0.1:5672
    exchange:
      name: %s
      kind: direct
      durable: true
      autoDelete: false
    queue:
      name: %s
      durable: true
      autoDelete: false
      exclusive: false
    consumer:
      tag: simple-consumer
      autoAck: false
      exclusive: false
routes:
  web:
    protocol: http
    endpoint: http://localhost:9999
    contentType: application/json
  notADestination:
    protocol: http
    endpoint: http://localhost:1234
    contentType: application/json
retry:
  protocol: mockRetry
  topic: retry
  channel: transit_retry
  hostname: 127.0.0.1:4150
  nsqlookupd: 127.0.0.1:4161
  jitter:
    attempts: 10
    min: 1000
    max: 10000`, exchange, queueName))

		viper.ReadConfig(bytes.NewBuffer(config))
		domain.ConfigureLogger(viper.GetViper())
		fmt.Println("Test started\n\n\n\n\n")
	})

	AfterEach(func() {
		fmt.Println("Test finished\n\n\n\n\n")

		//time.Sleep(5 * time.Second)
	})

	It("should load the configuration and start the services", func(done Done) {
		router := pkg.NewRouter(ctx)
		//go router.Run()

		Expect(router.AllRoutes()).To(ConsistOf("notadestination", "web", "_retry"))
		Expect(router.AllServices()).To(ConsistOf("rabbit", "_retry"))

		cancel()
		<-ctx.Done()
		router.Close()
		close(done)
	})

	It("should properly route messages from rabbit to http", func(done Done) {
		router := pkg.NewRouter(ctx)
		go router.Run()
		expected := &domain.Message{
			Body:        []byte(`Hello World!`),
			Destination: "web",
		}

		server := &http.Server{Addr: ":9999"}
		http.HandleFunc("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Println("in the handler func")
			bytes, err := ioutil.ReadAll(r.Body)
			Expect(err).To(BeNil())

			message := &domain.Message{}
			err = proto.Unmarshal(bytes, message)
			Expect(err).To(BeNil())
			Expect(message).To(Equal(expected))
		}))

		go func() {
			domain.Logger().Info("Starting http server")
			domain.Logger().Info("Listen and serve finished!", zap.String("error", server.ListenAndServe().Error()))
		}()

		body, err := proto.Marshal(expected)
		Expect(err).To(BeNil())
		Expect(publish(body)).To(Succeed())

		cancel()
		<-ctx.Done()
		server.Close()
		close(done)
	}, 5)

	It("should retry messages correctly", func(done Done) {
		router := pkg.NewRouter(ctx)
		go router.Run()

		expected := &domain.Message{
			Body:        []byte(`Hello World again!`),
			Destination: "notADestination",
		}

		body, err := proto.Marshal(expected)
		Expect(err).To(BeNil())
		Expect(publish(body)).To(Succeed())

		time.Sleep(1 * time.Second)

		Expect(domain.MockRetryRoute.Retries).To(Equal(11))
		Expect(domain.MockRetryService.Retries).To(Equal(10))
		cancel()
		<-ctx.Done()
		close(done)
	}, 5)
})

func publish(body []byte) error {
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	if err := channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	var queue amqp.Queue
	if queue, err = channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	);
		err != nil {
		return err
	}

	if err = channel.Publish(
		"",
		queue.Name,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{},
			Body:    body,
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

	return nil
}
