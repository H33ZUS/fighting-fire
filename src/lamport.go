package main

import (
	"sync"
)

type LamportClock struct {
	time int
	mu   sync.Mutex
}

func NewLamportClock() *LamportClock {
	return &LamportClock{
		time: 0,
	}
}

func (lc *LamportClock) Tick() int {

	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.time++
	return lc.time
}

func (lc *LamportClock) Update(recievedTime int) int {

	lc.mu.Lock()
	defer lc.mu.Unlock()

	if recievedTime > lc.time {
		lc.time = recievedTime
	}

	lc.time++
	return lc.time
}

func (lc *LamportClock) GetTime() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	return lc.time
}
