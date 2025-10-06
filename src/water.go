package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const MaxVolume = 1000

type WaterManager struct {
	volume          int
	connections     int
	refillRate      int
	consumptionRate int
	bus             MessageBus
	mu              sync.Mutex
}

func NewWaterManager(bus MessageBus) *WaterManager {

	wm := &WaterManager{
		volume:          1000,
		connections:     0,
		refillRate:      50,
		consumptionRate: 10,
		bus:             bus,
	}
	wm.SetupNatsSubscriptions()
	return wm
}

func (wm *WaterManager) SendStatusRespone(msg *nats.Msg, truckID int, status bool) {
	if msg.Reply == "" {
		log.Printf("WM WARNING: Message from Truck %d has no reply subject.", truckID)
		return
	}

	resp := WaterStatusResponse{TruckID: truckID, Status: status}
	data, _ := json.Marshal(resp)

	if err := msg.Respond(data); err != nil {
		log.Printf("WM Error responding to Truck %d: %v", truckID, err)
	}
}

func (wm *WaterManager) SetupNatsSubscriptions() {
	if err := wm.bus.Subscribe(SubjectWaterConnect, func(msg *nats.Msg) {
		var req ConnectionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			log.Printf("WM Error unmarshaling connect request: %v", err)
			return
		}

		wm.mu.Lock()
		wm.connections++
		wm.mu.Unlock()
		fmt.Printf("💧 Manager processed CONNECT for Truck %d. Active: %d\n", req.TruckID, wm.connections)

		wm.SendStatusRespone(msg, req.TruckID, true)

	}); err != nil {
		log.Fatalf("FATAL: WM failed to subscribe to %s: %v", SubjectWaterConnect, err)
	}

	if err := wm.bus.Subscribe(SubjectWaterDisconnect, func(msg *nats.Msg) {
		var req ConnectionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			log.Printf("WM Error unmarshaling disconnect request: %v", err)
			return
		}

		wm.mu.Lock()
		if wm.connections > 0 {
			wm.connections--
		}
		wm.mu.Unlock()
		fmt.Printf("💧 Manager processed DISCONNECT for Truck %d. Active: %d\n", req.TruckID, wm.connections)

		wm.SendStatusRespone(msg, req.TruckID, false)
	}); err != nil {
		log.Fatalf("FATAL: WM failed to subscribe to %s: %v", SubjectWaterDisconnect, err)
	}
}

func (wm *WaterManager) RefillWaterSupply(done <-chan struct{}) { // done channel to make sure the routine stops after closing the program
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.mu.Lock()

			if wm.volume < MaxVolume {
				newVolume := wm.volume + wm.refillRate

				if newVolume > MaxVolume {
					wm.volume = MaxVolume
				} else {
					wm.volume = newVolume
				}
			}

			if wm.connections > 0 {
				// For now, just print the required consumption,
				// actual consumption/allocation logic will go here later.
				required := wm.connections * wm.consumptionRate
				_ = required
			}

			wm.mu.Unlock()

		case <-done:
			return
		}
	}
}
