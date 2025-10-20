package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"fightingfire/grid"
	"fightingfire/helper"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var idCounter int = 1

var dx = [...]int{0, 0, -1, 1}
var dy = [...]int{-1, 1, 0, 0}

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
	clock          *LamportClock
	knownFires     map[string]FireEvent
}
type object struct {
	x    int
	y    int
	item int
}

const NatsRequestTimeout = 20 * time.Second

func fireKey(x, y int) string {
	return fmt.Sprintf("%d, %d", x, y)
}

func CreateFiretruck(bus MessageBus, gridsize int, positionX int, positionY int, updateInterval time.Duration, fm *FireManager) *FireTruck {
	currentID := idCounter
	idCounter++
	ft := &FireTruck{
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
		clock:          NewLamportClock(),
		knownFires:     make(map[string]FireEvent),
	}
	ft.SetupNatsSubscriptions()
	return ft
}

func (ft *FireTruck) PublishNewFire(x, y int) {
	event := FireEvent{
		X:               x,
		Y:               y,
		Timestamp:       ft.clock.Tick(),
		TruckID:         ft.id,
		ClaimingTruckID: 0,
	}

	data, _ := json.Marshal(event)
	fmt.Printf("[Truck %d] PUBLISHING NEW fire at (%d, %d)\n", ft.id, x, y)

	if err := ft.bus.Publish(SubjectNewFires, data); err != nil {
		fmt.Printf("Truck %d ERROR publishing new fire event: %v\n", ft.id, err)
	}
}

func (ft *FireTruck) PublishExFire(x, y int) {
	event := FireEvent{
		X:               x,
		Y:               y,
		Timestamp:       ft.clock.Tick(),
		TruckID:         ft.id,
		ClaimingTruckID: ft.id,
	}

	data, _ := json.Marshal(event)
	fmt.Printf("[Truck %d] PUBLISHING fire at (%d, %d) is EXTINGUISH\n", ft.id, x, y)

	if err := ft.bus.Publish(SubjectExFires, data); err != nil {
		fmt.Printf("Truck %d ERROR publishing ex fire event: %v\n", ft.id, err)
	}
}

func (ft *FireTruck) PublishFireClaim(event FireEvent) {
	ft.clock.Tick()
	event.Timestamp = ft.clock.GetTime()

	data, _ := json.Marshal(event)
	fmt.Printf("[Truck %d] CLAIMING fire at (%d, %d) Timestamp: %d\n", ft.id, event.X, event.Y, event.Timestamp)

	if err := ft.bus.Publish(SubjectClaimFire, data); err != nil {
		fmt.Printf("Truck %d ERROR publishing fire claim event: %v\n", ft.id, err)
	}
}

func (ft *FireTruck) FireUpdateHandler(msg *nats.Msg) {
	var event FireEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		log.Printf("FT Error unmarshaling fire event from %s: %v", msg.Subject, err)
		return
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	key := fireKey(event.X, event.Y)
	ft.clock.Update(event.Timestamp)

	currentEvent, exists := ft.knownFires[key]

	if !exists || event.Timestamp > currentEvent.Timestamp {
		ft.knownFires[key] = event

		if event.ClaimingTruckID != 0 && currentEvent.ClaimingTruckID == 0 {
			fmt.Printf("[Truck %d] Fire (%d, %d) CLAIMED by Truck %d. Time: %d\n", ft.id, event.X, event.Y, event.ClaimingTruckID, event.Timestamp)
		} else if msg.Subject == SubjectNewFires && !exists {
			fmt.Printf("[Truck %d] Learned of NEW fire at (%d, %d). Time: %d\n", ft.id, event.X, event.Y, event.Timestamp)
		}
	}

}

func (ft *FireTruck) SetupNatsSubscriptions() {
	if err := ft.bus.Subscribe(SubjectNewFires, ft.FireUpdateHandler); err != nil {
		log.Fatalf("FATAL: [Truck %d] failed to subscribe to %s: %v", ft.id, SubjectNewFires, err)
	}

	if err := ft.bus.Subscribe(SubjectClaimFire, ft.FireUpdateHandler); err != nil {
		log.Fatalf("FATAL: [Truck %d] failed to subscribe to %s: %v", ft.id, SubjectClaimFire, err)
	}

	if err := ft.bus.Subscribe(SubjectExFires, func(msg *nats.Msg) {
		var event FireEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			log.Printf("FT Error unmarshaling SubjectExFires request: %v", err)
			return
		}

		ft.mu.Lock()
		delete(ft.knownFires, fireKey(event.X, event.Y))
		ft.clock.Update(event.Timestamp)
		ft.mu.Unlock()

		fmt.Printf("[Truck %d] Learned fire at (%d, %d) is EXTINGUISHED. Fires: %d\n", ft.id, event.X, event.Y, len(ft.knownFires))
	}); err != nil {
		log.Fatalf("FATAL: [Truck %d] failed to subscribe to %s: %v", ft.id, SubjectExFires, err)
	}
}

// TEMPORARY FUNCTION TO CHECK IF FIRE IS NEARBY --- NEEDED FOR FIRE EXTINGUISHING LOGIC
func (ft *FireTruck) isFireNearby() (x, y int) {
	// Acquire a read lock on the grid before reading its state
	grid.GridMutex.RLock() // Assuming gridMutex is defined globally in main.go or similar
	defer grid.GridMutex.RUnlock()

	ft.mu.Lock()
	defer ft.mu.Unlock()

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
	timestamp := ft.clock.Tick()
	ft.mu.Lock()
	if ft.hasWater || ft.isConnecting {
		ft.mu.Unlock()
		return
	}

	ft.isConnecting = true
	ft.mu.Unlock()

	req := ConnectionRequest{TruckID: ft.id, Timestamp: timestamp}
	data, _ := json.Marshal(req)

	fmt.Printf("[Truck %d] REQUEST water at time %d\n", ft.id, timestamp)

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
	timestamp := ft.clock.Tick()
	req := ConnectionRequest{TruckID: ft.id, Timestamp: timestamp}
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
	objects := ft.vision(ft.visionRange)

	ft.mu.RLock()
	workingStatus := ft.isworking
	hasWater := ft.hasWater
	isConnecting := ft.isConnecting
	fireDetectedGlobally := len(ft.knownFires) > 0
	ft.mu.RUnlock()

	if fireDetectedGlobally && !workingStatus {
		ft.mu.Lock()
		ft.isworking = true
		ft.mu.Unlock()
		workingStatus = true
	}

	if workingStatus && !hasWater && !isConnecting {
		go ft.RequestWaterConnection()
	}

	fireX, fireY := ft.isFireNearby()
	fireDetectedLocally := fireX != -2 // -2 out of bounds

	if workingStatus && fireDetectedLocally && hasWater {
		grid.GridMutex.Lock()

		ft.fm.ExtinguishFire(fireX, fireY)

		if (grid.Grid)[fireX][fireY].Intensity <= 0 {
			(grid.Grid)[fireX][fireY].HasFire = false
			grid.GridMutex.Unlock()

			timestamp := ft.clock.Tick()
			fmt.Printf("[Truck %d] EXTINGUISH fire at (%d,%d) TimeStamp: %d\n", ft.id, fireX, fireY, timestamp)

			ft.PublishExFire(fireX, fireY)
		} else {
			grid.GridMutex.Unlock()
		}
	}

	if len(objects) == 0 {
		ft.freeMove([]int{0, 1, 2, 3})
	} else {
		ft.guidedMove(objects)
	}

	if workingStatus && !fireDetectedGlobally {
		if hasWater {
			go ft.DisconnectWaterRequest()
		}

		ft.mu.Lock()
		ft.isworking = false
		ft.mu.Unlock()
	}
}

func (ft *FireTruck) vision(distance int) []object {

	grid.GridMutex.RLock()
	defer grid.GridMutex.RUnlock()

	ft.mu.Lock()
	defer ft.mu.Unlock()

	var objects []object

	for i := -distance; i <= distance; i++ {
		for j := -distance; j <= distance; j++ {
			x := i + ft.positionX
			y := j + ft.positionY
			if x >= 0 && x < 20 && y >= 0 && y < 20 && !(x == ft.positionX) && !(y == ft.positionY) {
				if grid.Grid[x][y].HasFire {
					objects = append(objects, object{x, y, 0})

					key := fireKey(x, y)

					if _, known := ft.knownFires[key]; !known {
						ft.PublishNewFire(x, y)

						ft.knownFires[key] = FireEvent{X: x, Y: y, Timestamp: ft.clock.GetTime(), TruckID: ft.id}
					}
				} else if grid.Grid[x][y].HasTruck {
					objects = append(objects, object{x, y, 1})
				}
			}
		}
	}
	return objects
}

func (ft *FireTruck) move(target helper.Object, state int) {

	timestamp := ft.clock.Tick()

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

	fmt.Printf("[Tuck %d] MOVE to (%d,%d) TimeStamp: %d\n", ft.id, newX, newY, timestamp)

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

	ft.mu.RLock()

	var bestFireTarget helper.Object
	var fireToClaim *FireEvent
	minDistance := math.MaxInt

	for _, fireEvent := range ft.knownFires {
		if fireEvent.ClaimingTruckID != 0 && fireEvent.ClaimingTruckID != ft.id {
			continue
		}

		dist := ft.distance(fireEvent.X, fireEvent.Y)

		if dist < minDistance {
			minDistance = dist
			bestFireTarget = helper.Object{X: fireEvent.X, Y: fireEvent.Y, Priority: dist}

			if fireEvent.ClaimingTruckID == 0 {
				fireToClaim = &fireEvent
			} else {
				fireToClaim = nil
			}
		}
	}

	ft.mu.RUnlock()

	if minDistance != math.MaxInt {
		if fireToClaim != nil {
			claimEvent := *fireToClaim
			claimEvent.ClaimingTruckID = ft.id
			ft.PublishFireClaim(claimEvent)
		}

		ft.move(bestFireTarget, 0)
		ft.setPos(ft.positionX, ft.positionY)
		return
	}

	pqtrucks := &helper.ObjectHeap{}
	heap.Init(pqtrucks)

	for _, obj := range target {
		if obj.item == 1 {
			heap.Push(pqtrucks, helper.Object{X: obj.x, Y: obj.y, Priority: ft.distance(obj.x, obj.y)})
		}
	}

	if pqtrucks.Len() > 0 {
		m := heap.Pop(pqtrucks).(helper.Object)

		ft.move(m, 1)
		ft.setPos(ft.positionX, ft.positionY)
	}
}

// move function for firetrucks not on job
func (ft *FireTruck) freeMove(options []int) {

	// Clear current position
	ft.clearPos()

	// Random direction: up, down, left, right
	moved := false

	for len(options) > 0 {
		dirIndex := rand.Intn(len(options))
		chosenDir := options[dirIndex]

		if chosenDir < 0 || chosenDir >= len(dx) {
			options = helper.RemoveDirection(options, dirIndex)
			continue
		}

		newX := ft.positionX + dx[chosenDir]
		newY := ft.positionY + dy[chosenDir]

		// Stay within bounds
		if ft.checkPos(newX, newY) {
			ft.positionX = newX
			ft.positionY = newY
			moved = true
			break
		} else {
			options = helper.RemoveDirection(options, dirIndex)
		}
	}

	// Set new position
	ft.setPos(ft.positionX, ft.positionY)

	if !moved && len(options) > 0 {
		fmt.Printf("[Truck %d] Failed to move\n", ft.id)
	}
}
