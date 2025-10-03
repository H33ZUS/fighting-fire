package main

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

type MessageBus interface {
	Publish(subject string, data []byte) error
	Subscribe(subject string, handler func(msg []byte)) error
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

func (b *NatsBus) Subscribe(subject string, handler func(msg []byte)) error {
	_, err := b.nc.Subscribe(subject, func(m *nats.Msg) {
		handler(m.Data)
	})
	return err
}

func (b *NatsBus) Close() {
	if b.nc != nil {
		b.nc.Close()
		log.Println("❌ NATS connection closed.")
	}
}
