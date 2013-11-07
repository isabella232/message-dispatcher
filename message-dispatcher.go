package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"strings"
)

var (
	queueUri       string
	queueName      string
	apiUri         string
	maxDispatchers uint
)

func init() {
	flag.StringVar(&queueUri, "from", queueUri, "RabbitMQ uri")
	flag.StringVar(&queueName, "q", queueName, "queue name")
	flag.StringVar(&apiUri, "to", apiUri, "forward to api")
	flag.UintVar(&maxDispatchers, "#", 1, "maximum number of concurrent dispatchers")
}

func main() {
	flag.Parse()
	if err := consumeQueue(queueUri, queueName); err != nil {
		log.Fatal(err)
	}
}

func consumeQueue(queueUri string, queueName string) error {
	conn, err := amqp.Dial(queueUri)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	deliveries, err := channel.Consume(queueName, "", false, true, true, false, nil)
	if err != nil {
		if err, ok := err.(*amqp.Error); ok {
			if err.Code == amqp.AccessRefused && strings.HasSuffix(err.Reason, "in exclusive use") {
				return nil
			}
		}
		return err
	}

	log.Printf("connected to: %q", queueUri)
	log.Printf("consuming queue: %q", queueName)
	errors := make(chan error)
	for i := uint(0); i < maxDispatchers; i++ {
		go handleDeliveries(deliveries, errors)
	}

	return <-errors
}

func handleDeliveries(deliveries <-chan amqp.Delivery, errors chan error) {
	for delivery := range deliveries {
		if err := handleDelivery(delivery); err != nil {
			errors <- fmt.Errorf("%s: %#v", err, string(delivery.Body))
			return
		}

		if err := delivery.Ack(false); err != nil {
			errors <-fmt.Errorf("Error acknowledging message: %s", err)
			return
		}
	}
}

func handleDelivery(delivery amqp.Delivery) error {
	resp, err := http.Post(apiUri, delivery.ContentType, bytes.NewReader(delivery.Body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Received %d HTTP response when forwarding message: %s", resp.StatusCode, delivery.Body)
	}

	return nil
}
