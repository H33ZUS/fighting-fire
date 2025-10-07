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
	fm             *FireManager
	isConnecting   bool
}

const NatsRequestTimeout = 1 * time.Second

func CreateFiretruck(bus MessageBus, grid *[][]Cell, gridsize int, positionX int, positionY int, updateInterval time.Duration, fm *FireManager) *FireTruck {
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
		fm:             fm,
		isConnecting:   false,
	}
}

// TEMPORARY FUNCTION TO CHECK IF FIRE IS NEARBY --- NEEDED FOR FIRE EXTINGUISHING LOGIC
func (ft *FireTruck) isFireNearby() (x, y int) {
	// Acquire a read lock on the grid before reading its state
	gridMutex.RLock() // Assuming gridMutex is defined globally in main.go or similar
	defer gridMutex.RUnlock()

	// Define the 8 surrounding directions (including diagonals)
	directions := []struct{ dx, dy int }{{0, 1}, {0, -1}, {1, 0}, {-1, 0}, {1, 1}, {1, -1}, {-1, 1}, {-1, -1}}
	//directions := []struct{ dx, dy int }{{0, 1}, {0, -1}, {1, 0}, {-1, 0}}

	for _, d := range directions {
		nx, ny := ft.positionX+d.dx, ft.positionY+d.dy

		// Check if the coordinates are valid and if the cell has fire
		// We use (*ft.grid) to access the underlying grid slice
		if nx >= 0 && nx < ft.gridsize && ny >= 0 && ny < ft.gridsize {
			if (*ft.grid)[nx][ny].HasFire {
				return nx, ny
			}
		}
	}
	return -2, -1 // hardcoded values so it is out of range
}

func (ft *FireTruck) RequestWaterConnection() {
	ft.mu.Lock()
	if ft.hasWater || ft.isConnecting {
		ft.mu.Unlock()
		return
	}

	ft.isConnecting = true
	ft.mu.Unlock()

	req := ConnectionRequest{TruckID: ft.id}
	data, _ := json.Marshal(req)

	// fmt.Printf("🚒 Truck %d requesting connection on %s...\n", ft.id, SubjectWaterConnect)

	msg, err := ft.bus.Request(SubjectWaterConnect, data, NatsRequestTimeout)

	defer func() {
		ft.mu.Lock()
		ft.isConnecting = false
		ft.mu.Unlock()
	}()

	if err != nil {
		fmt.Printf("Truck %d ERROR requesting connection (NATS error): %v. Will retry.\n", ft.id, err)
		return
	}

	var resp WaterStatusResponse

	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		fmt.Printf("Truck %d Error unmarshalling status response: %v. Will retry.\n", ft.id, err)
		return
	}

	ft.mu.Lock()
	ft.hasWater = resp.Status
	ft.mu.Unlock()
	/*
		if resp.Status {
			fmt.Printf("✅ Truck %d CONNECTED (Reply received). hasWater=%t\n", ft.id, resp.Status)
		} else {
			fmt.Printf("❌ Truck %d DENIED connection (Reply received). hasWater=%t. Will retry on next cycle.\n", ft.id, resp.Status)
		}
	*/
}

func (ft *FireTruck) DisconnectWaterRequest() {
	req := ConnectionRequest{TruckID: ft.id}
	data, _ := json.Marshal(req)

	// fmt.Printf("🚒 Truck %d requesting disconnection on %s...\n", ft.id, SubjectWaterDisconnect)

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
	/*
		if !resp.Status {
			fmt.Printf("❌ Truck %d DISCONNECTED (Reply received). hasWater=%t\n", ft.id, resp.Status)
		} else {
			fmt.Printf("⚠️ Truck %d DISCONNECT warning (Reply received status: true). hasWater=%t\n", ft.id, resp.Status)
		}
	*/
}

func (ft *FireTruck) initial(ctx context.Context) {
	ticker := time.NewTicker(ft.updateInterval)
	defer ticker.Stop()

	ft.spawn()

	for {
		select {
		case <-ctx.Done():
			// Check to make sure that if a truck spawns and hasWater is set to true to disconnect it from the water supply
			if ft.hasWater {
				ft.DisconnectWaterRequest()
			}
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
	ft.mu.RLock()
	workingStatus := ft.isworking
	hasWater := ft.hasWater
	isConnecting := ft.isConnecting
	ft.mu.RUnlock()

	// temporarily checks if a fire is nearby
	fireX, fireY := ft.isFireNearby()
	fireDetected := fireX != -2

	if fireDetected && !workingStatus {
		ft.mu.Lock()
		ft.isworking = true
		ft.mu.Unlock()

		workingStatus = true
	}

	if workingStatus && !hasWater && !isConnecting {
		go ft.RequestWaterConnection()
	}

	if workingStatus && fireDetected && hasWater {
		ft.fm.ExtinguishFire(fireX, fireY)
		// fmt.Printf("💦 Truck %d extinguishing fire at (%d, %d)\n", ft.id, fireX, fireY)
	}

	if workingStatus && !fireDetected {
		if hasWater {
			go ft.DisconnectWaterRequest()
		}

		ft.mu.Lock()
		ft.isworking = false
		ft.mu.Unlock()
	}
}
