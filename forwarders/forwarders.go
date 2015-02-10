package forwarders

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type Forwarder interface {
	Forward(delivery amqp.Delivery) error
}

func ForwardDeliveries(deliveries <-chan amqp.Delivery, forwarder Forwarder, consumerId uint, consumeTimeout time.Duration, requeueFailed bool, returnOnFailed bool, quit <-chan error, errs chan<- error) {
	timeout := make(<-chan time.Time)

	for {
		if consumeTimeout > 0 {
			timeout = time.After(consumeTimeout)
		}

		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}

			if apiErr := forwarder.Forward(delivery); apiErr != nil {
				errs <- fmt.Errorf("(consumer: %d) %s. Delivery message: %#v", consumerId, apiErr, string(delivery.Body))

				if nackErr := delivery.Nack(false, requeueFailed); nackErr != nil {
					errs <- fmt.Errorf("(consumer: %d) error rejecting message: %s", consumerId, nackErr, string(delivery.Body))
				}

				if returnOnFailed {
					return
				}
			} else if err := delivery.Ack(false); err != nil {
				errs <- fmt.Errorf("(consumer: %d) error acknowledging message: %s", consumerId, err)
				return
			}
		case <-timeout:
			return
		case err := <-quit:
			errs <- fmt.Errorf("(consumer: %d) %s", consumerId, err)
			return
		}
	}
}

type HttpForwarder struct {
	url string
}

func NewHttpForwarder(url *url.URL) *HttpForwarder {
	return &HttpForwarder{url: url.String()}
}

func (httpFw *HttpForwarder) Forward(delivery amqp.Delivery) error {
	resp, err := http.Post(httpFw.url, delivery.ContentType, bytes.NewReader(delivery.Body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("received %d HTTP response when forwarding message", resp.StatusCode)
	}

	return nil
}

type SocketForwarder struct {
	url *url.URL
}

func NewSocketForwarder(url *url.URL) *SocketForwarder {
	return &SocketForwarder{url: url}
}

func (sf *SocketForwarder) Forward(delivery amqp.Delivery) error {
	socket, err := net.Dial("unix", sf.url.Opaque)
	if err != nil {
		return err
	}
	defer socket.Close()

	_, err = io.Copy(socket, bytes.NewReader(delivery.Body))
	if err != nil {
		return fmt.Errorf("Error forwarding message to socket: %s", err)
	}

	return nil
}
