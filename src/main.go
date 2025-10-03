package main

import (
	"context"
	"fmt"
	"time"
)

type Cell struct {
	hasFire   bool
	hasTruck  bool
	intensity int
}

var grid [][]Cell

func main() {

	gridSize := 20
	grid = make([][]Cell, gridSize)

	for i := range grid {
		grid[i] = make([]Cell, gridSize)
	}
	// fmt.Println(grid)

	fmt.Println("Starting...")

	ctx, cancel := context.WithCancel(context.Background()) // creates root context and makes it cancellable
	defer cancel()                                          // ensures cancel() is called when main exits

	waterManagerDone := make(chan struct{})             // creates new channel for water supply for refilling
	waterManager := NewWaterManager()                   // creates new water supply
	go waterManager.RefillWaterSupply(waterManagerDone) // runs refill routine

	fireManager := NewFireManager(&grid, gridSize)
	fireManager.Start(ctx) // starts spawn goroutine, fire spawns every 5 sec

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeout := time.After(30 * time.Second) // channel that signals after 30 sec

	for {
		select {
		case <-timeout:
			fmt.Println("\nSimulation ended")
			close(waterManagerDone)            // closes water supply channel
			cancel()                           // stop all goroutines
			time.Sleep(500 * time.Millisecond) // give time for clean up
			return

		case <-ticker.C: // counts fires every 2 sec
			count := 0
			for i := 0; i < gridSize; i++ {
				for j := 0; j < gridSize; j++ {
					if grid[i][j].hasFire {
						count++
					}
				}
			}
			fmt.Printf("Active fires %d\n ", count)

			// access current volume of water supply for printing purpose
			waterManager.mu.Lock()
			currentVolume := waterManager.volume
			waterManager.mu.Unlock()
			fmt.Printf("Water Supply %d\n ", currentVolume)

		}
	}
}
