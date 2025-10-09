package main

import (
	"context"
	"fightingfire/grid"
	"fmt"
	"math/rand"
	"time"
)

type FireManager struct {
	gridSize       int
	spawnInterval  time.Duration
	spreadInterval time.Duration
	growthInterval time.Duration
	maxFires       int
	spreadProb     float64 // probability of fires spreading to neighbour
	fireCount      int
	clock          *LamportClock
}

const FireExtinguishRate = 3

func NewFireManager(gridSize int) *FireManager {

	return &FireManager{
		gridSize:       gridSize,
		spawnInterval:  5 * time.Second,
		spreadInterval: 4 * time.Second,
		growthInterval: 6 * time.Second,
		maxFires:       10,
		spreadProb:     0.3,
		clock:          NewLamportClock(),
	}
}

func (fm *FireManager) Start(ctx context.Context) {

	go fm.spawnLoop(ctx) // context used to start and end goroutines
	go fm.growthLoop(ctx)
	go fm.spreadLoop(ctx)

}

func (fm *FireManager) spawnLoop(ctx context.Context) {

	ticker := time.NewTicker(fm.spawnInterval)
	defer ticker.Stop()

	for {
		select { // waits for whichever channel sends signal first
		case <-ctx.Done(): // signal here = goroutine exits
			return
		case <-ticker.C: // otherwise spawn fire
			fm.tryRandomSpawnFire()
		}
	}
}

func (fm *FireManager) growthLoop(ctx context.Context) {

	ticker := time.NewTicker(fm.growthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fm.growFires()
		}
	}
}

func (fm *FireManager) spreadLoop(ctx context.Context) {

	ticker := time.NewTicker(fm.spreadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fm.spreadFires()
		}
	}
}

func (fm *FireManager) isValidCoordinate(x, y int) bool {
	return x >= 0 && x < fm.gridSize && y >= 0 && y < fm.gridSize
}

func (fm *FireManager) tryRandomSpawnFire() {

	if fm.countActiveFires() >= fm.maxFires {
		return
	}

	for i := 0; i < 10; i++ { // we try spawning for 10 times, otherwise skip for this tick
		x := rand.Intn(fm.gridSize)
		y := rand.Intn(fm.gridSize)

		if !fm.hasAdjacentFire(x, y) && fm.spawnFire(x, y) {
			return
		}

	}
}

func (fm *FireManager) HasFire(x, y int) bool {

	grid.GridMutex.RLock()
	defer grid.GridMutex.RUnlock()

	if !fm.isValidCoordinate(x, y) {
		return false
	}
	return (grid.Grid)[x][y].HasFire
}

func (fm *FireManager) spawnFire(x, y int) bool {
	grid.GridMutex.Lock()
	defer grid.GridMutex.Unlock() // locks the grid and unlocks it when this func returns

	if !fm.isValidCoordinate(x, y) {
		return false
	}

	if (grid.Grid)[x][y].HasFire {
		return false
	}

	(grid.Grid)[x][y].HasFire = true
	(grid.Grid)[x][y].Intensity = 1
	fm.fireCount++
	return true
}

func (fm *FireManager) countActiveFires() int {

	grid.GridMutex.RLock()
	defer grid.GridMutex.RUnlock()

	return fm.fireCount
}

func (fm *FireManager) hasAdjacentFire(x, y int) bool {

	grid.GridMutex.RLock()
	defer grid.GridMutex.RUnlock()

	directions := []struct{ dx, dy int }{
		{-1, 0}, {1, 0}, {0, -1}, {0, 1}, {-1, -1}, {-1, 1}, {1, -1}, {1, 1},
	}

	for _, d := range directions {
		nx, ny := x+d.dx, y+d.dy
		if fm.isValidCoordinate(nx, ny) && (grid.Grid)[nx][ny].HasFire {
			return true
		}
	}
	return false
}

func (fm *FireManager) growFires() {

	grid.GridMutex.RLock()
	defer grid.GridMutex.RUnlock()

	for i := 0; i < fm.gridSize; i++ {
		for j := 0; j < fm.gridSize; j++ {
			if (grid.Grid)[i][j].HasFire {
				(grid.Grid)[i][j].Intensity *= 2
			}
		}
	}

}

func (fm *FireManager) spreadFires() {

	grid.GridMutex.Lock()
	defer grid.GridMutex.Unlock()

	var fireLocations [][2]int

	for i := 0; i < fm.gridSize; i++ {
		for j := 0; j < fm.gridSize; j++ {
			if (grid.Grid)[i][j].HasFire {
				fireLocations = append(fireLocations, [2]int{i, j})
			}
		}
	}

	for _, loc := range fireLocations {
		fm.spreadFrom(loc[0], loc[1])
	}
}

func (fm *FireManager) spreadFrom(x, y int) {

	neighbors := [][2]int{
		{x - 1, y},
		{x + 1, y},
		{x, y - 1},
		{x, y + 1},
	}

	for _, n := range neighbors { // spread with probability if neighbor doesn't have a fire
		nx, ny := n[0], n[1]

		if !fm.isValidCoordinate(nx, ny) {
			continue
		}

		if !(grid.Grid)[nx][ny].HasFire && rand.Float64() < fm.spreadProb {
			(grid.Grid)[nx][ny].HasFire = true
			(grid.Grid)[nx][ny].Intensity = 1
		}
	}
}

func (fm *FireManager) getIntensity(x, y int) int {

	grid.GridMutex.RLock()
	defer grid.GridMutex.RUnlock()

	if !fm.isValidCoordinate(x, y) || !(grid.Grid)[x][y].HasFire {
		return 0
	}

	return (grid.Grid)[x][y].Intensity
}

func (fm *FireManager) ExtinguishFire(x, y int) {
	timestamp := fm.clock.Tick()
	// if there is no fire to extinguish (false function call)
	if !fm.isValidCoordinate(x, y) || !(grid.Grid)[x][y].HasFire {
		return
	}

	fmt.Printf("EXTINGUISH fire at (%d,%d) at time %d\n", x, y, timestamp)

	(grid.Grid)[x][y].Intensity -= FireExtinguishRate

	if (grid.Grid)[x][y].Intensity <= 0 {
		(grid.Grid)[x][y].HasFire = false
		(grid.Grid)[x][y].Intensity = 0
		fm.fireCount--
		// fmt.Printf("🔥 Fire extinguished at (%d, %d)\n", x, y)
	}
}
