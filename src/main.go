package main

import (
	"context"
	"fightingfire/grid"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gosuri/uilive"
)

func main() {

	natsURL := "nats://127.0.0.1:4222"
	bus, err := NewNatsBus(natsURL)
	if err != nil {
		log.Fatalf("FATAL: Failed to connect to NATS server at %s: %v", natsURL, err)
	}
	defer bus.Close()

	consoleWriter := uilive.New()
	consoleWriter.Start()

	gridSize := 20
	grid.Grid = make([][]grid.Cell, gridSize)

	for i := range grid.Grid {
		grid.Grid[i] = make([]grid.Cell, gridSize)
	}
	// fmt.Println(grid)

	fmt.Println("Starting...")

	ctx, cancel := context.WithCancel(context.Background()) // creates root context and makes it cancellable
	defer cancel()                                          // ensures cancel() is called when main exits

	waterManagerDone := make(chan struct{})             // creates new channel for water supply for refilling
	waterManager := NewWaterManager(bus)                // creates new water supply
	go waterManager.RefillWaterSupply(waterManagerDone) // runs refill routine

	fireManager := NewFireManager(gridSize)
	fireManager.Start(ctx) // starts spawn goroutine, fire spawns every 5 sec

	for i := 0; i < 10; i++ {
		x := rand.Intn(20)
		y := rand.Intn(20)

		if !grid.Grid[x][y].HasTruck {
			firetrucks := CreateFiretruck(bus, gridSize, x, y, 1, fireManager)

			go firetrucks.initial(ctx)
		} else {
			i--
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.After(60 * time.Second) // channel that signals after 30 sec

	for {
		select {
		case <-timeout:
			fmt.Println("\nSimulation ended")
			cancel()                           // stop all goroutines
			time.Sleep(500 * time.Millisecond) // give time for clean up
			return

		case <-ticker.C: // Updating the scene based on ticker interval
			count := 0
			for i := 0; i < gridSize; i++ {
				for j := 0; j < gridSize; j++ {
					if grid.Grid[i][j].HasFire {
						count++
					}
				}
			}

			grid.GridMutex.RLock()
			//Draws the state of the simulation
			size := len(grid.Grid)
			var state string

			for i := 0; i < size; i++ {
				for j := 0; j < size; j++ {
					var cell string
					if grid.Grid[i][j].HasFire {
						cell = "\033[31m🔥 \033[0m"
					} else if grid.Grid[i][j].HasTruck {
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

			// FOR TESTING PURPOSES
			// fmt.Printf("\n")
			// fmt.Printf("%s", state)

			// access current volume of water supply for printing purpose
			waterManager.mu.Lock()
			currentVolume := waterManager.volume
			waterManager.mu.Unlock()
			fmt.Fprintf(consoleWriter, "Water Supply %d\n ", currentVolume)

			grid.GridMutex.RUnlock()
		}
	}
}
