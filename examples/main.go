package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"time"
)

var (
	p *nsq.Producer
)

func init() {
	var err error
	p, err = nsq.NewProducer("127.0.0.1:4150", nsq.NewConfig())
	if err != nil {
		panic(err)
	}
}

func main() {
	go consume("test", "sub01")
	go consume("test", "sub02")
	produce("test", 100)
}

func produce(topic string, n int) {
	var err error
	for i := 0; i < n; i++ {
		if err = p.Publish(topic, []byte(fmt.Sprintf("MSG_%d", i))); err != nil {
			panic(err)
		}
		time.Sleep(100 * time.Millisecond)
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
	consumer.AddHandler(nsq.HandlerFunc(
		func(msg *nsq.Message) error {
			fmt.Printf("%s -> %q\n", subName, msg.Body)
			return nil
		}))

	if err := consumer.ConnectToNSQLookupd("127.0.0.1:4161"); err != nil {
		panic(err)
	}
	<-consumer.StopChan
}
