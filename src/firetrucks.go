package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"fightingfire/grid"
	"fightingfire/helper"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

var idCounter int = 1

type FireTruck struct {
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
	visionRange    int
	mu             sync.RWMutex
}
type object struct {
	x    int
	y    int
	item int
}

const NatsRequestTimeout = 1 * time.Second

func CreateFiretruck(bus MessageBus, gridsize int, positionX int, positionY int, updateInterval time.Duration, fm *FireManager) *FireTruck {
	currentID := idCounter
	idCounter++
	return &FireTruck{
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
		visionRange:    5,
	}
}

// TEMPORARY FUNCTION TO CHECK IF FIRE IS NEARBY --- NEEDED FOR FIRE EXTINGUISHING LOGIC
func (ft *FireTruck) isFireNearby() (x, y int) {
	// Acquire a read lock on the grid before reading its state
	grid.GridMutex.RLock() // Assuming gridMutex is defined globally in main.go or similar
	defer grid.GridMutex.RUnlock()

	// Define the 8 surrounding directions (including diagonals)
	directions := []struct{ dx, dy int }{{0, 1}, {0, -1}, {1, 0}, {-1, 0}, {1, 1}, {1, -1}, {-1, 1}, {-1, -1}}
	//directions := []struct{ dx, dy int }{{0, 1}, {0, -1}, {1, 0}, {-1, 0}}

	for _, d := range directions {
		nx, ny := ft.positionX+d.dx, ft.positionY+d.dy

		// Check if the coordinates are valid and if the cell has fire
		// We use (*ft.grid) to access the underlying grid slice
		if nx >= 0 && nx < ft.gridsize && ny >= 0 && ny < ft.gridsize {
			if (grid.Grid)[nx][ny].HasFire {
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
	grid.GridMutex.Lock()
	defer grid.GridMutex.Unlock()

	ft.setPos(ft.positionX, ft.positionY)
	return true
}

func (ft *FireTruck) clearPos() {
	grid.Grid[ft.positionX][ft.positionY].HasTruck = false
}

func (ft *FireTruck) setPos(x int, y int) {
	grid.Grid[x][y].HasTruck = true
}

func (ft *FireTruck) checkPos(newX int, newY int) bool {
	if (newX >= 0 && newX < ft.gridsize && newY >= 0 && newY < ft.gridsize) && (!(grid.Grid)[newX][newY].HasTruck && !(grid.Grid)[newX][newY].HasFire) {
		return true
	} else {
		return false
	}
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

	grid.GridMutex.Lock()
	defer grid.GridMutex.Unlock()

	if workingStatus && fireDetected && hasWater {
		ft.fm.ExtinguishFire(fireX, fireY)
		// fmt.Printf("💦 Truck %d extinguishing fire at (%d, %d)\n", ft.id, fireX, fireY)
	}

	objects := ft.vision(ft.visionRange)

	if len(objects) == 0 {
		ft.freeMove([]int{0, 1, 2, 3})
	} else {
		ft.guidedMove(objects)
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

func (ft *FireTruck) vision(distance int) []object {

	var objects []object

	for i := -distance; i <= distance; i++ {
		for j := -distance; j <= distance; j++ {
			x := i + ft.positionX
			y := j + ft.positionY
			if x >= 0 && x < 20 && y >= 0 && y < 20 && !(x == ft.positionX) && !(y == ft.positionY) {
				if grid.Grid[x][y].HasFire {
					objects = append(objects, object{x, y, 0})
				} else if grid.Grid[x][y].HasTruck {
					objects = append(objects, object{x, y, 1})
				}
			}
		}
	}
	return objects
}

func (ft *FireTruck) move(target helper.Object, state int) {

	vecX := target.X - ft.positionX
	vecY := target.Y - ft.positionY

	if math.Abs(float64(vecX)) <= 1 && math.Abs(float64(vecY)) <= 1 {
		ft.isworking = true
		return
	}
	//maybe here

	stepX, stepY := 0, 0
	if math.Abs(float64(vecX)) >= math.Abs(float64(vecY)) {
		// Move horizontally
		if vecX != 0 {
			stepX = vecX / int(math.Abs(float64(vecX)))
			stepY = 0
		}
	} else {
		if vecY != 0 {
			stepY = vecY / int(math.Abs(float64(vecY)))
			stepX = 0
		}
	}

	switch state {
	case 1:
		stepX *= -1
		stepY *= -1
	case 2:
		ft.freeMove([]int{0, 1, 2, 3})
		return
	}

	newX := ft.positionX + stepX
	newY := ft.positionY + stepY

	if ft.checkPos(newX, newY) {
		ft.clearPos()
		ft.positionX = newX
		ft.positionY = newY
	} else {
		ft.move(target, state+1)
	}
}

func (ft *FireTruck) distance(x int, y int) int {
	dx := x - ft.positionX
	dy := y - ft.positionY
	return int(math.Abs(float64(dx)) + math.Abs(float64(dy)))
}

func (ft *FireTruck) guidedMove(target []object) {

	pqfires := &helper.ObjectHeap{}
	heap.Init(pqfires)

	pqtrucks := &helper.ObjectHeap{}
	heap.Init(pqtrucks)

	for i := 0; i < len(target); i++ {
		switch target[i].item {
		case 0:
			heap.Push(pqfires, helper.Object{X: target[i].x, Y: target[i].y, Priority: ft.distance(target[i].x, target[i].y)})
		case 1:
			heap.Push(pqtrucks, helper.Object{X: target[i].x, Y: target[i].y, Priority: ft.distance(target[i].x, target[i].y)})
		}
	}

	if pqfires.Len() > 0 {
		m := heap.Pop(pqfires).(helper.Object)

		ft.move(m, 0)

		ft.setPos(ft.positionX, ft.positionY)
	} else {
		m := heap.Pop(pqtrucks).(helper.Object)

		ft.move(m, 1)
		// Set new position
		ft.setPos(ft.positionX, ft.positionY)
	}
}

// move function for firetrucks not on job
func (ft *FireTruck) freeMove(options []int) {
	// Clear current position
	ft.clearPos()

	// Random direction: up, down, left, right
	dx := []int{0, 0, -1, 1}
	dy := []int{-1, 1, 0, 0}
	dir := rand.Intn(len(options))

	newX := ft.positionX + dx[options[dir]]
	newY := ft.positionY + dy[options[dir]]

	// Stay within bounds
	if ft.checkPos(newX, newY) {
		ft.positionX = newX
		ft.positionY = newY
	} else {
		helper.RemoveDirection(options, dir)
		ft.freeMove(options)
	}

	// Set new position
	ft.setPos(ft.positionX, ft.positionY)
}
