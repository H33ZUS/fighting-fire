package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

/*
------------------!!!BEFORE RUNNING NATS!!!---------------------

 1. make sure that docker desktop is installed
 2. if so open a terminal and type in the following command:
    docker run -d --name nats-server -p 4222:4222 nats:latest

----------------------------------------------------------------
*/
type MessageBus interface {
	Publish(subject string, data []byte) error
	Subscribe(subject string, handler func(m *nats.Msg)) error
	Request(subject string, data []byte, timeout time.Duration) (*nats.Msg, error)
	Close()
}

type NatsBus struct {
	nc *nats.Conn
}

func NewNatsBus(natsURL string) (MessageBus, error) {
	nc, err := nats.Connect(natsURL)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}
	log.Println("✅ Successfully connected to NATS.") // Confirmation log
	return &NatsBus{nc: nc}, nil
}

func (b *NatsBus) Publish(subject string, data []byte) error {
	return b.nc.Publish(subject, data)
}

func (b *NatsBus) Subscribe(subject string, handler func(m *nats.Msg)) error {
	_, err := b.nc.Subscribe(subject, handler)
	return err
}

func (b *NatsBus) Request(subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	return b.nc.Request(subject, data, timeout)
}

func (b *NatsBus) Close() {
	if b.nc != nil {
		b.nc.Close()
		log.Println("❌ NATS connection closed.")
	}
}
