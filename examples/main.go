package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"time"
)

const TOPIC = "test"

func main() {
	go consume(TOPIC, "sub01")
	go consume(TOPIC, "sub02")
	go produce(TOPIC, "127.0.0.1:4150", 100)
	go produce(TOPIC, "127.0.0.1:4152", 100)
	time.Sleep(2 * time.Minute)
}

func produce(topic, addr string, n int) {
	p, err := nsq.NewProducer(addr, nsq.NewConfig())
	if err != nil {
		panic(err)
	}
	for i := 0; i < n; i++ {
		if err = p.Publish(topic, []byte(fmt.Sprintf("MSG_%d", i))); err != nil {
			panic(err)
		}
		time.Sleep(1000 * time.Millisecond)
		fmt.Println("send ->", i)
	}
}

func consume(topic, subName string) {
	cfg := nsq.NewConfig()
	cfg.DialTimeout = 10 * time.Second
	consumer, err := nsq.NewConsumer(topic, subName, cfg)
	if err != nil {
		panic(err)
	}
	consumer.SetLoggerLevel(nsq.LogLevelDebug)
	consumer.AddHandler(nsq.HandlerFunc(
		func(msg *nsq.Message) error {
			fmt.Printf("recv: %s -> %q\n", subName, msg.Body)
			return nil
		}))

	if err := consumer.ConnectToNSQLookupd("127.0.0.1:4161"); err != nil {
		panic(err)
	}
	<-consumer.StopChan
}
