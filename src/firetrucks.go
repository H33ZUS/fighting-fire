package main

import (
	"container/heap"
	"context"
	"fightingfire/grid"
	"fightingfire/helper"
	priorityheap "fightingfire/helper"
	"math"
	"math/rand"
	"time"
)

type fireTruck struct {
	gridsize       int
	positionX      int
	positionY      int
	isworking      bool
	updateInterval time.Duration
	visionRange    int
}

type object struct {
	x    int
	y    int
	item int
}

func CreateFiretruck(grid *[][]grid.Cell, gridsize int, positionX int, positionY int, updateInterval time.Duration) *fireTruck {
	return &fireTruck{
		gridsize:       gridsize,
		positionX:      positionX,
		positionY:      positionY,
		isworking:      false,
		updateInterval: updateInterval * time.Second,
		visionRange:    5,
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
	grid.GridMutex.Lock()
	defer grid.GridMutex.Unlock()

	ft.setPos(ft.positionX, ft.positionY)
	return true
}

func (ft *fireTruck) clearPos() {
	grid.Grid[ft.positionX][ft.positionY].HasTruck = false
}

func (ft *fireTruck) setPos(x int, y int) {
	grid.Grid[x][y].HasTruck = true
}

func (ft *fireTruck) checkPos(newX int, newY int) bool {
	if (newX >= 0 && newX < ft.gridsize && newY >= 0 && newY < ft.gridsize) && (!(grid.Grid)[newX][newY].HasTruck && !(grid.Grid)[newX][newY].HasFire) {
		return true
	} else {
		return false
	}
}

func (ft *fireTruck) update() {
	grid.GridMutex.Lock()
	defer grid.GridMutex.Unlock()

	objects := ft.vision(ft.visionRange)

	if len(objects) == 0 {
		ft.freeMove([]int{0, 1, 2, 3})
	} else {
		ft.guidedMove(objects)
	}

}

func (ft *fireTruck) vision(distance int) []object {

	var objects []object

	for i := -distance; i <= distance; i++ {
		for j := -distance; j <= distance; j++ {
			x := i + ft.positionX
			y := j + ft.positionY
			if x >= 0 && x < 20 && y >= 0 && y < 20 && !(x == ft.positionX) && !(y == ft.positionY) {
				if grid.Grid[x][y].HasFire {
					objects = append(objects, object{x, y, 0})
				} else if grid.Grid[x][y].HasTruck {
					objects = append(objects, object{x, y, 1})
				}
			}
		}
	}
	return objects
}

func (ft *fireTruck) move(target helper.Object, state int) {

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

	if ft.checkPos(newX, newY) {
		ft.clearPos()
		ft.positionX = newX
		ft.positionY = newY
	} else {
		ft.move(target, state+1)
	}
}

func (ft *fireTruck) distance(x int, y int) int {
	dx := x - ft.positionX
	dy := y - ft.positionY
	return int(math.Abs(float64(dx)) + math.Abs(float64(dy)))
}

func (ft *fireTruck) guidedMove(target []object) {

	pqfires := &priorityheap.ObjectHeap{}
	heap.Init(pqfires)

	pqtrucks := &priorityheap.ObjectHeap{}
	heap.Init(pqtrucks)

	for i := 0; i < len(target); i++ {
		switch target[i].item {
		case 0:
			heap.Push(pqfires, priorityheap.Object{X: target[i].x, Y: target[i].y, Priority: ft.distance(target[i].x, target[i].y)})
		case 1:
			heap.Push(pqtrucks, priorityheap.Object{X: target[i].x, Y: target[i].y, Priority: ft.distance(target[i].x, target[i].y)})
		}
	}

	if pqfires.Len() > 0 {
		m := heap.Pop(pqfires).(priorityheap.Object)

		ft.move(m, 0)

		ft.setPos(ft.positionX, ft.positionY)
	} else {
		m := heap.Pop(pqtrucks).(priorityheap.Object)

		ft.move(m, 1)
		// Set new position
		ft.setPos(ft.positionX, ft.positionY)
	}
}

// move function for firetrucks not on job
func (ft *fireTruck) freeMove(options []int) {
	// Clear current position
	ft.clearPos()

	// Random direction: up, down, left, right
	dx := []int{0, 0, -1, 1}
	dy := []int{-1, 1, 0, 0}
	dir := rand.Intn(len(options))

	newX := ft.positionX + dx[options[dir]]
	newY := ft.positionY + dy[options[dir]]

	// Stay within bounds
	if ft.checkPos(newX, newY) {
		ft.positionX = newX
		ft.positionY = newY
	} else {
		helper.RemoveDirection(options, dir)
		ft.freeMove(options)
	}

	// Set new position
	ft.setPos(ft.positionX, ft.positionY)
}
