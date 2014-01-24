package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"strings"
	"os"
	"os/signal"
	"syscall"
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

	deliveries, err := getDeliveries(queueUri, queueName)
	if err != nil {
		log.Fatal(err)
	}
	if deliveries == nil {
		os.Exit(1)
	}

	log.Printf("connected to: %q", queueUri)
	log.Printf("consuming queue: %q", queueName)

	errors := consumeDeliveries(deliveries, maxDispatchers)
	for i := uint(0); i < maxDispatchers; i++ {
		log.Printf("%s", <-errors)
	}
	os.Exit(1)
}

func getDeliveries(queueUri string, queueName string) (<-chan amqp.Delivery, error) {
	conn, err := amqp.Dial(queueUri)
	if err != nil {
		return nil, err
	}
	go handleSigQuit(conn)

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	deliveries, err := channel.Consume(queueName, "", false, true, true, false, nil)
	if err != nil {
		if err, ok := err.(*amqp.Error); ok {
			if err.Code == amqp.AccessRefused && strings.HasSuffix(err.Reason, "in exclusive use") {
				return nil, nil
			}
		}
		return nil, err
	}

	return deliveries, nil
}

func consumeDeliveries(deliveries <-chan amqp.Delivery, maxDispatchers uint) <-chan error {
	errors := make(chan error)
	for i := uint(0); i < maxDispatchers; i++ {
		go handleDeliveries(deliveries, i, errors)
	}
	return errors
}

func handleDeliveries(deliveries <-chan amqp.Delivery, handlerNumber uint, errors chan<- error) {
	for delivery := range deliveries {
		if err := handleDelivery(delivery); err != nil {
			errors <- fmt.Errorf("%s: %#v", err, string(delivery.Body))
			return
		}
		if err := delivery.Ack(false); err != nil {
			errors <-fmt.Errorf("error acknowledging message: %s", err)
			return
		}
	}

	errors <-fmt.Errorf("handler %d disconnected from RabbitMQ server", handlerNumber)
}

func handleDelivery(delivery amqp.Delivery) error {
	resp, err := http.Post(apiUri, delivery.ContentType, bytes.NewReader(delivery.Body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("received %d HTTP response when forwarding message: %s", resp.StatusCode, delivery.Body)
	}

	return nil
}

func handleSigQuit(conn *amqp.Connection) {
	quitSignal := make(chan os.Signal, 1)
	signal.Notify(quitSignal, syscall.SIGQUIT)
	<-quitSignal
	log.Printf("received QUIT signal\n")
	conn.Close()
}