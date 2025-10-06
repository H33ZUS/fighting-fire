package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

var idCounter int = 1

type FireTruck struct {
	grid           *[][]Cell
	mu             sync.RWMutex
	id             int
	bus            MessageBus
	gridsize       int
	positionX      int
	positionY      int
	isworking      bool
	updateInterval time.Duration
	hasWater       bool
}

const NatsRequestTimeout = 1 * time.Second

func CreateFiretruck(bus MessageBus, grid *[][]Cell, gridsize int, positionX int, positionY int, updateInterval time.Duration) *FireTruck {
	currentID := idCounter
	idCounter++

	return &FireTruck{
		grid:           grid,
		id:             currentID,
		bus:            bus,
		gridsize:       gridsize,
		positionX:      positionX,
		positionY:      positionY,
		isworking:      false,
		updateInterval: updateInterval * time.Second,
		hasWater:       false,
	}
}

func (ft *FireTruck) RequestWaterConnection() {
	req := ConnectionRequest{TruckID: ft.id}
	data, _ := json.Marshal(req)

	fmt.Printf("🚒 Truck %d requesting connection on %s...\n", ft.id, SubjectWaterConnect)

	respMsg, err := ft.bus.Request(SubjectWaterConnect, data, NatsRequestTimeout)

	if err != nil {
		fmt.Printf("Truck %d ERROR requesting connect: %v\n", ft.id, err)

		ft.mu.Lock()
		ft.hasWater = false
		ft.mu.Unlock()

		return
	}

	var resp WaterStatusResponse

	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		fmt.Printf("Truck %d Error unmarshalling status response: %v\n", ft.id, err)
		return
	}

	ft.mu.Lock()
	ft.hasWater = resp.Status
	ft.mu.Unlock()

	if resp.Status {
		fmt.Printf("✅ Truck %d CONNECTED (Reply received). hasWater=%t\n", ft.id, resp.Status)
	} else {
		fmt.Printf("❌ Truck %d CONNECTION FAILED (Reply received). hasWater=%t\n", ft.id, resp.Status)
	}
}

func (ft *FireTruck) DisconnectWaterRequest() {
	req := ConnectionRequest{TruckID: ft.id}
	data, _ := json.Marshal(req)

	fmt.Printf("🚒 Truck %d requesting disconnection on %s...\n", ft.id, SubjectWaterDisconnect)

	respMsg, err := ft.bus.Request(SubjectWaterDisconnect, data, NatsRequestTimeout)

	if err != nil {
		fmt.Printf("Truck %d ERROR requesting disconnect: %v\n", ft.id, err)
		return
	}

	var resp WaterStatusResponse

	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		fmt.Printf("Truck %d Error unmarshalling status response: %v\n", ft.id, err)
		return
	}

	ft.mu.Lock()
	ft.hasWater = resp.Status
	ft.mu.Unlock()

	if !resp.Status {
		fmt.Printf("❌ Truck %d DISCONNECTED (Reply received). hasWater=%t\n", ft.id, resp.Status)
	} else {
		fmt.Printf("⚠️ Truck %d DISCONNECT warning (Reply received status: true). hasWater=%t\n", ft.id, resp.Status)
	}
}

func (ft *FireTruck) initial(ctx context.Context) {
	ticker := time.NewTicker(ft.updateInterval)
	defer ticker.Stop()

	ft.spawn()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ft.update()
		}
	}
}

func (ft *FireTruck) spawn() bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	ft.setPos(ft.positionX, ft.positionY)
	return true
}

func (ft *FireTruck) clearPos() {
	(*ft.grid)[ft.positionX][ft.positionY].hasTruck = false
}

func (ft *FireTruck) setPos(x int, y int) {
	(*ft.grid)[x][y].hasTruck = true
}

func (ft *FireTruck) update() {
}
