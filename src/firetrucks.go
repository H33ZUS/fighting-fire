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
}

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
	}
}

func (ft *FireTruck) RequestWaterConnection() {
	req := ConnectionRequest{TruckID: ft.id}
	data, _ := json.Marshal(req)

	// Publish request
	if err := ft.bus.Publish(SubjectWaterConnect, data); err != nil {
		fmt.Printf("Truck %d ERROR publishing connect: %v\n", ft.id, err)
	} else {
		fmt.Printf("🚒 Truck %d published connection request.\n", ft.id)
	}
}

func (ft *FireTruck) DisconnectWaterRequest() {
	req := ConnectionRequest{TruckID: ft.id}
	data, _ := json.Marshal(req)

	// Publish request
	if err := ft.bus.Publish(SubjectWaterDisconnect, data); err != nil {
		fmt.Printf("Truck %d ERROR publishing disconnect: %v\n", ft.id, err)
	} else {
		fmt.Printf("🚒 Truck %d published disconnection request.\n", ft.id)
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
