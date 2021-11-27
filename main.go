package main

import (
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// change to your environment
	// user guest is only allow to connect from localhost
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	handleError(err, "Failed to connect to RabbitMQ server")
	defer conn.Close()

	ch, err := conn.Channel()
	handleError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // type
		false,   //  auto-deleted
		false,   // internal
		false,   // noWait
		nil,     // arguments
	)
	handleError(err, "Failed to declare queue")

	// msgs is a channel type Delivery
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	handleError(err, "Failed to register a consumer")

	// clever trick to wait
	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}

	}()

	var body []byte
	if len(os.Args) > 1 {
		body = []byte(strings.Join(os.Args[1:], " "))
	} else {
		body = []byte("Hello World!!!")
	}

	for i := 0; ; i++ {
		err = ch.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        body,
			})
		handleError(err, "Failed to publish a message")

		log.Printf("Publish a message: %s", body)
		time.Sleep(5 * time.Second)
	}

	log.Printf(" [*] Waiting for messages. To exit press Ctrl+C")
	<-forever
}
