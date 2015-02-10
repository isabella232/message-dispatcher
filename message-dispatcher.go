package main

import (
	"flag"
	"fmt"
	"github.com/nuts/message-dispatcher/forwarders"
	"github.com/streadway/amqp"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	queueUrl       string
	queueName      string
	apiUrl         string
	numDispatchers uint
	consumeTimeout time.Duration
	requeueFailed  bool
	exitOnFailed   bool
	verbose        bool
)

func init() {
	flag.StringVar(&queueUrl, "from", queueUrl, "RabbitMQ url")
	flag.StringVar(&queueName, "q", queueName, "queue name")
	flag.StringVar(&apiUrl, "to", apiUrl, "forward to api url")
	flag.UintVar(&numDispatchers, "#", 1, "number of concurrent dispatchers")
	flag.DurationVar(&consumeTimeout, "t", consumeTimeout, "stop consuming when no messages arrive before the timeout")
	flag.BoolVar(&requeueFailed, "requeue-failed", false, "requeue messages that failed to forward")
	flag.BoolVar(&exitOnFailed, "exit-on-failed", false, "terminate the dispatcher when a forward fails")
	flag.BoolVar(&verbose, "v", false, "verbose")
}

func main() {
	flag.Parse()

	forwarder, err := getForwarder(apiUrl)
	if err != nil {
		log.Fatal(err)
	}
	channel, err := getChannel(queueUrl)
	if err != nil {
		log.Fatal(err)
	}
	deliveries, err := getDeliveries(channel, queueName)
	if err != nil {
		log.Fatal(err)
	}
	if deliveries == nil {
		os.Exit(1)
	}

	if verbose {
		log.Printf("connected to: %q", queueUrl)
		log.Printf("consuming queue: %s", queueName)
		log.Printf("forwarding to: %#v\n", apiUrl)
		log.Printf("number of dispatchers: %d", numDispatchers)
		if consumeTimeout > 0 {
			log.Printf("consume timeout: %v", consumeTimeout)
		} else {
			log.Printf("consume timeout: none")
		}
		log.Printf("requeuing messages on failure: %v", requeueFailed)
		log.Printf("terminating dispatchers on failure: %v", requeueFailed)
	}

	errors := consumeDeliveries(deliveries, forwarder, numDispatchers, consumeTimeout, requeueFailed, exitOnFailed)
	exitStatus := 0

	for error := range errors {
		exitStatus = 1
		log.Printf("%s", error)
	}

	os.Exit(exitStatus)
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

func getChannel(queueUrl string) (*amqp.Channel, error) {
	conn, err := amqp.Dial(queueUrl)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return channel, nil
}

func getDeliveries(channel *amqp.Channel, queueName string) (<-chan amqp.Delivery, error) {
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

func consumeDeliveries(deliveries <-chan amqp.Delivery, forwarder forwarders.Forwarder, numDispatchers uint, consumeTimeout time.Duration, requeueFailed bool, returnOnFailed bool) <-chan error {
	var wg sync.WaitGroup
	errors := make(chan error)
	quit := make(chan error)

	handleSignals(numDispatchers, quit)

	for i := uint(0); i < numDispatchers; i++ {
		go func(consumerId uint) {
			wg.Add(1)
			forwarders.ForwardDeliveries(deliveries, forwarder, consumerId, consumeTimeout, requeueFailed, returnOnFailed, quit, errors)
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		close(errors)
	}()

	return errors
}

func handleSignals(numDispatchers uint, quit chan<- error) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGQUIT, syscall.SIGINT)
	go func() {
		sig := <-signals
		for i := uint(0); i < numDispatchers; i++ {
			quit <- fmt.Errorf("quitting on %s signal\n", sig)
		}
	}()
}
