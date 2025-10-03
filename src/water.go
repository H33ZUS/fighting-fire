package main

import (
	"sync"
	"time"
)

const MaxVolume = 1000

type WaterManager struct {
	volume          int
	connections     int
	refillRate      int
	consumptionRate int
	mu              sync.Mutex
}

func NewWaterManager() *WaterManager {

	return &WaterManager{
		volume:          100,
		connections:     0,
		refillRate:      50,
		consumptionRate: 10,
	}
}

func (wm *WaterManager) RefillWaterSupply(done <-chan struct{}) { // done channel to make sure the routine stops after closing the program
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.mu.Lock()

			if wm.volume < MaxVolume {
				newVolume := wm.volume + wm.refillRate

				if newVolume > MaxVolume {
					wm.volume = MaxVolume
				} else {
					wm.volume = newVolume
				}
			} else {

			}
			wm.mu.Unlock()

		case <-done:
			return
		}
	}
}
