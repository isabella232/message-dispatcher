package forwarders

import (
  "bytes"
  "fmt"
  "github.com/streadway/amqp"
  "net/http"
  "net/url"
  "io"
  "net"
)

type Forwarder interface {
  Forward(delivery amqp.Delivery) error
}

func ForwardDeliveries(deliveries <-chan amqp.Delivery, forwarder Forwarder, handlerId uint, errors chan<- error) {
  for delivery := range deliveries {
    if err := forwarder.Forward(delivery); err != nil {
      errors <- fmt.Errorf("%s. Delivery message: %#v", err, string(delivery.Body))
      return
    }
    if err := delivery.Ack(false); err != nil {
      errors <-fmt.Errorf("error acknowledging message: %s", err)
      return
    }
  }

  errors <-fmt.Errorf("handler %d disconnected from RabbitMQ server", handlerId)
}

type HttpForwarder struct {
  url string
}

func NewHttpForwarder(url *url.URL) *HttpForwarder {
  return &HttpForwarder{ url: url.String() }
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
  return &SocketForwarder{ url: url }
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