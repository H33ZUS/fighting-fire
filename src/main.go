package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gosuri/uilive"
)

type Cell struct {
	HasFire   bool
	hasTruck  bool
	intensity int
}

var grid [][]Cell

func main() {

	consoleWriter := uilive.New()
	consoleWriter.Start()

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

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.After(60 * time.Second) // channel that signals after 30 sec

	for {
		select {
		case <-timeout:
			fmt.Println("\nSimulation ended")
			close(waterManagerDone)            // closes water supply channel
			cancel()                           // stop all goroutines
			time.Sleep(500 * time.Millisecond) // give time for clean up
			return

		case <-ticker.C: // Updating the scene based on ticker interval
			count := 0
			for i := 0; i < gridSize; i++ {
				for j := 0; j < gridSize; j++ {
					if grid[i][j].HasFire {
						count++
					}
				}
			}

			//Draws the state of the simulation
			size := len(grid)
			var state string

			for i := 0; i < size; i++ {
				for j := 0; j < size; j++ {
					var cell string
					if grid[i][j].HasFire {
						cell = "\033[31m🔥 \033[0m"
					} else if grid[i][j].hasTruck {
						cell = "🚒 "
					} else {
						cell = "\033[32m🌲 \033[0m"
					}
					state += cell
				}
				state += "\n"
			}

			fmt.Fprintf(consoleWriter, "\n")
			fmt.Fprintf(consoleWriter, "%s", state)
			fmt.Fprintf(consoleWriter, "Active fires %d\n ", count)

			// access current volume of water supply for printing purpose
			waterManager.mu.Lock()
			currentVolume := waterManager.volume
			waterManager.mu.Unlock()
			fmt.Fprintf(consoleWriter, "Water Supply %d\n ", currentVolume)

		}
	}
}
