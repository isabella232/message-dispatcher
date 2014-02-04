package main

import (
	"flag"
	"fmt"
	"github.com/nuts/message-dispatcher/forwarders"
	"github.com/streadway/amqp"
	"log"
	"strings"
	"os"
	"os/signal"
	"net/url"
	"syscall"
)

var (
	queueUrl       string
	queueName      string
	apiUrl         string
	maxDispatchers uint
)

func init() {
	flag.StringVar(&queueUrl, "from", queueUrl, "RabbitMQ url")
	flag.StringVar(&queueName, "q", queueName, "queue name")
	flag.StringVar(&apiUrl, "to", apiUrl, "forward to api url")
	flag.UintVar(&maxDispatchers, "#", 1, "maximum number of concurrent dispatchers")
}

func main() {
	flag.Parse()

	deliveries, err := getDeliveries(queueUrl, queueName)
	if err != nil {
		log.Fatal(err)
	}
	if deliveries == nil {
		os.Exit(1)
	}
	log.Printf("connected to: %q", queueUrl)

	forwarder, err := getForwarder(apiUrl)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("forwarding to: %#v\n", apiUrl)

	errors := consumeDeliveries(deliveries, forwarder, maxDispatchers)
	for i := uint(0); i < maxDispatchers; i++ {
		log.Printf("%s", <-errors)
	}
	os.Exit(1)
}

func getForwarder(apiUrl string) (forwarders.Forwarder, error) {
	apiEndpoint, err := url.Parse(apiUrl)
	if err != nil {
		return nil, err
	}

	switch apiEndpoint.Scheme {
	case "unix":
		return forwarders.NewSocketForwarder(apiEndpoint), nil
	case "http", "https":
		return forwarders.NewHttpForwarder(apiEndpoint), nil
	}

	return nil, fmt.Errorf("invalid api url: %s", apiUrl)
}

func getDeliveries(queueUrl string, queueName string) (<-chan amqp.Delivery, error) {
	conn, err := amqp.Dial(queueUrl)
	if err != nil {
		return nil, err
	}
	go handleSignals(conn)

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

func consumeDeliveries(deliveries <-chan amqp.Delivery, forwarder forwarders.Forwarder, maxDispatchers uint) <-chan error {
	errors := make(chan error)
	for i := uint(0); i < maxDispatchers; i++ {
		go forwarders.ForwardDeliveries(deliveries, forwarder, i, errors)
	}
	return errors
}

func handleSignals(conn *amqp.Connection) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGQUIT, syscall.SIGINT)
	sig := <-signals
	log.Printf("received %s signal\n", sig)
	conn.Close()
}