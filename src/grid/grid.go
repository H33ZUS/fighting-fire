package grid

import "sync"

type Cell struct {
	HasFire   bool
	HasTruck  bool
	Intensity int
}

var Grid [][]Cell
var GridMutex sync.RWMutex
