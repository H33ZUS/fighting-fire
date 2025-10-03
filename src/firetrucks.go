package main

import (
	"context"
	"sync"
	"time"
)

type fireTruck struct {
	grid           *[][]Cell
	mu             sync.RWMutex
	gridsize       int
	positionX      int
	positionY      int
	isworking      bool
	updateInterval time.Duration
}

func CreateFiretruck(grid *[][]Cell, gridsize int, positionX int, positionY int, updateInterval time.Duration) *fireTruck {
	return &fireTruck{
		grid:           grid,
		gridsize:       gridsize,
		positionX:      positionX,
		positionY:      positionY,
		isworking:      false,
		updateInterval: updateInterval * time.Second,
	}
}

func (ft *fireTruck) initial(ctx context.Context) {
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

func (ft *fireTruck) spawn() bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	ft.setPos(ft.positionX, ft.positionY)
	return true
}

func (ft *fireTruck) clearPos() {
	(*ft.grid)[ft.positionX][ft.positionY].hasTruck = false
}

func (ft *fireTruck) setPos(x int, y int) {
	(*ft.grid)[x][y].hasTruck = true
}

func (ft *fireTruck) update() {
}
