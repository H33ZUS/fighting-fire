package main

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type FireManager struct {
	grid           *[][]Cell
	mu             sync.RWMutex
	gridSize       int
	spawnInterval  time.Duration
	spreadInterval time.Duration
	growthInterval time.Duration
	maxFires       int
	spreadProb     float64 // probability of fires spreading to neighbour
	fireCount      int
}

func NewFireManager(grid *[][]Cell, gridSize int) *FireManager {

	return &FireManager{
		grid:           grid,
		gridSize:       gridSize,
		spawnInterval:  5 * time.Second,
		spreadInterval: 3 * time.Second,
		growthInterval: 2 * time.Second,
		maxFires:       10,
		spreadProb:     0.3,
	}
}

func (fm *FireManager) Start(ctx context.Context) {

	go fm.spawnLoop(ctx) // context used to start and end goroutines

	// more go routines to be added

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

func (fm *FireManager) isValidCoordinate(x, y int) bool {
	return x >= 0 && x < fm.gridSize && y >= 0 && y < fm.gridSize
}

func (fm *FireManager) hasFire(x, y int) bool {
	fm.mu.RLock()
	defer fm.mu.RUnlock() // allows multiple goroutines to read at once

	if !fm.isValidCoordinate(x, y) {
		return false
	}
	return (*fm.grid)[x][y].hasFire
}

func (fm *FireManager) tryRandomSpawnFire() {

	if fm.countActiveFires() >= fm.maxFires {
		return
	}

	x := rand.Intn(fm.gridSize)
	y := rand.Intn(fm.gridSize)

	fm.spawnFire(x, y)
}

func (fm *FireManager) spawnFire(x, y int) bool {
	fm.mu.Lock()
	defer fm.mu.Unlock() // locks the grid and unlocks it when this func returns

	if !fm.isValidCoordinate(x, y) {
		return false
	}

	if (*fm.grid)[x][y].hasFire {
		return false
	}

	(*fm.grid)[x][y].hasFire = true
	(*fm.grid)[x][y].intensity = 1
	fm.fireCount++
	return true

}

func (fm *FireManager) countActiveFires() int {

	fm.mu.RLock()
	defer fm.mu.RUnlock()

	return fm.fireCount
}
