package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

// Configuration File Opjects
type Configuration struct {
	Broker         string
	BrokerUser     string
	BrokerPwd      string
	BrokerQueue    string
	BrokerVhost    string
	BrokerExchange string
	LogIP          string
	LogPort        string
	LogSrc         string
	LocalEcho      bool
	ServerName     string
	PubPool        int
}

type chanToRabbit struct {
	payload string
	route   string
}

var (
	rabbitConn       *amqp.Connection
	rabbitCloseError chan *amqp.Error
)

var amqpURI string
var conf Configuration
var chanPayload chanToRabbit
var alive, seed int
var messages chan chanToRabbit

//checkError function
func checkError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

//sendMessage to udp listener
func sendMessage(msg string) {

	if conf.LocalEcho {
		log.Println(msg)
	}
	ServerAddr, err := net.ResolveUDPAddr("udp", conf.LogIP+":"+conf.LogPort)
	checkError(err)

	LocalAddr, err := net.ResolveUDPAddr("udp", conf.LogSrc+":0")
	checkError(err)

	Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	checkError(err)

	defer Conn.Close()
	buf := []byte(msg)
	if _, err := Conn.Write(buf); err != nil {
		log.Println(msg, err)
	}
}

// Try to connect to the RabbitMQ server as
// long as it takes to establish a connection
//
func connectToRabbitMQ(uri string) *amqp.Connection {
	for {
		conn, err := amqp.Dial(uri)

		if err == nil {
			return conn
		}

		checkError(err)
		sendMessage("Trying to reconnect to RabbitMQ")
		time.Sleep(500 * time.Millisecond)
	}
}

// re-establish the connection to RabbitMQ in case
// the connection has died
//
func rabbitConnector(uri string) {
	var rabbitErr *amqp.Error

	for {
		rabbitErr = <-rabbitCloseError
		if rabbitErr != nil {
			sendMessage("Connecting to RabbitMQ")
			rabbitConn = connectToRabbitMQ(uri)
			rabbitCloseError = make(chan *amqp.Error)
			rabbitConn.NotifyClose(rabbitCloseError)
		}
	}
}

func main() {

	//Load Default Configuration Values
	conf.LocalEcho = true
	conf.LogIP = "127.0.0.1"
	conf.LogPort = "10001"
	conf.LogSrc = "127.0.0.1"

	//Load Configuration Data
	dat, _ := ioutil.ReadFile("conf.json")
	err := json.Unmarshal(dat, &conf)
	checkError(err)

	sendMessage("Golang - JSON to AMQP 1.0")

	amqpURI = "amqp://" + conf.BrokerUser + ":" + conf.BrokerPwd + "@" + conf.Broker + conf.BrokerVhost

	// go healthCheck()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	// create the rabbitmq error channel
	rabbitCloseError = make(chan *amqp.Error)

	// run the callback in a separate thread
	go rabbitConnector(amqpURI)

	// establish the rabbitmq connection by sending
	// an error and thus calling the error callback
	rabbitCloseError <- amqp.ErrClosed

	messages = make(chan chanToRabbit, 1024)

	for i := 0; i <= conf.PubPool; i++ {
		go func() {
			for {
				if rabbitConn != nil {
					chanPubToRabbit()
					log.Println("Lost Channel...")
				}
				time.Sleep(1 * time.Second)
			}
		}()
	}

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		sendMessage("Exiting Program, recieved: ")
		sendMessage(fmt.Sprintln(sig))
		done <- true
		os.Exit(0)
	}()

	//Create listener here.......

	http.HandleFunc("/", handler)
	http.ListenAndServe(":82", nil)

	sendMessage("Press Ctrl+C to exit: ")
	<-done
}

func handler(w http.ResponseWriter, r *http.Request) {

	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)
	s := buf.String()
	pubToRabbit(s, "p.mon.pcg.logs")
	fmt.Fprintf(w, "OK")
}

func pubToRabbit(payload string, route string) {

	var msg chanToRabbit
	msg.payload = payload
	msg.route = route
	messages <- msg
	return
}

func chanPubToRabbit() {

	ch, err := rabbitConn.Channel()
	checkError(err)
	if err == nil {
		defer ch.Close()

		for {
			msg := <-messages

			body := msg.payload
			err = ch.Publish(
				conf.BrokerExchange, // exchange
				msg.route,           // routing key
				false,               // mandatory
				false,               // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				})
			checkError(err)
			if err != nil {
				return
			}
		}
	} else {
		time.Sleep(10 * time.Second)
		return
	}

}

func stringEscape(szTemp string) string {
	if len(szTemp) > 1 {
		szTemp = strings.TrimSpace(fmt.Sprint(szTemp))
		szTemp = strings.Replace(szTemp, "\\", "\\\\", -1)
		szTemp = strings.Replace(szTemp, "\"", "\\\"", -1)
	}
	return szTemp
}
