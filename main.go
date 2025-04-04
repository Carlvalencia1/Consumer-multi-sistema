package main

import (
	"bytes"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s:%s", err, msg)
	}
}

func getPool() *amqp.Connection {

	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error loading .env file")
		return nil
	}

	user := os.Getenv("user_rabbit")
	password := os.Getenv("password_user")
	ip := os.Getenv("ip_instance")
	log.Printf("ip %s", user)

	conn, err := amqp.Dial("amqp://" + user + ":" + password + "@" + ip + "/")
	failOnError(err, "error to get connection")
	return conn
}

func sendMessagePatients(msg []byte) {

	reader := bytes.NewReader(msg)

	response, err := http.Post("http://localhost:8082/patients/", "application/json", reader)
	failOnError(err, "")

	log.Printf("response %s", response.Body)
}

func sendMessageExpediente(msg []byte) {
	reader := bytes.NewReader(msg)
	response, err := http.Post("http://localhost:8082/cases/", "application/json", reader)

	failOnError(err, "cant send the message")

	log.Printf("message %s", response.Body)
}

func requestCaja(message <-chan amqp.Delivery) {
	go func() {
		for msg := range message {
			sendMessageExpediente(msg.Body)
		}
	}()
}

func requestOrange(message <-chan amqp.Delivery) {
	go func() {
		for msg := range message {
			log.Printf("message patients %s", msg.Body)
			sendMessagePatients(msg.Body)
		}
	}()
}

func declareQueueOranges(ch *amqp.Channel) amqp.Queue {

	q, err := ch.QueueDeclare(
		"API2patients",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Fallo al declarar la cola")

	routingKey := "api2.oranges"
	err = ch.QueueBind(
		q.Name,
		routingKey,
		"amq.topic",
		false,
		nil,
	)
	failOnError(err, "Fallo al bindear la cola con el exchange")

	return q
}

func declareQueueCaja(ch *amqp.Channel) amqp.Queue {
	q, err := ch.QueueDeclare(
		"API2caja",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Fallo al declarar la cola")

	routingKey := "api2.cajas"
	err = ch.QueueBind(
		q.Name,
		routingKey,
		"amq.topic",
		false,
		nil,
	)
	failOnError(err, "Fallo al bindear la cola con el exchange")

	return q
}

func main() {
	conn := getPool()
	defer conn.Close()

	ch, errC := conn.Channel()

	failOnError(errC, "error to declare queue")

	defer ch.Close()

	q := declareQueueOranges(ch)
	qC := declareQueueCaja(ch)

	//consume message
	msg, errMsg := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	failOnError(errMsg, "error to consumer message")

	msgCaja, errC := ch.Consume(
		qC.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	failOnError(errC, "error to consumer caja queue")

	forever := make(chan bool)

	requestOrange(msg)
	requestCaja(msgCaja)

	<-forever
}
