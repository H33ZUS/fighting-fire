package main

import (
	"container/heap"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const MaxVolume = 1000

type PriorityQueue []*ConnectionRequest // for fairness

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {

	return pq[i].Timestamp < pq[j].Timestamp // smaller lamport timestamp = higher priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(*ConnectionRequest)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Peek() *ConnectionRequest {
	if pq.Len() == 0 {
		return nil
	}
	return (*pq)[0]
}

type WaterManager struct {
	volume          int
	connections     int
	refillRate      int
	consumptionRate int
	bus             MessageBus
	mu              sync.Mutex
	clock           *LamportClock
	queue           PriorityQueue
}

func NewWaterManager(bus MessageBus) *WaterManager {
	wm := &WaterManager{
		volume:          1000,
		connections:     0,
		refillRate:      50,
		consumptionRate: 20,
		bus:             bus,
		clock:           NewLamportClock(),
	}
	wm.SetupNatsSubscriptions()
	heap.Init(&wm.queue)
	go wm.ProcessQueue()
	return wm
}

func (wm *WaterManager) Enqueue(req *ConnectionRequest) {

	heap.Push(&wm.queue, req)
}

func (wm *WaterManager) Dequeue() *ConnectionRequest {

	if wm.queue.Len() == 0 {
		return nil
	}

	item := heap.Pop(&wm.queue).(*ConnectionRequest)
	return item
}

func (wm *WaterManager) SetupNatsSubscriptions() {

	if err := wm.bus.Subscribe(SubjectWaterConnect, func(msg *nats.Msg) {
		var req ConnectionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			log.Printf("WM Error unmarshaling connect request: %v", err)
			return
		}

		req.Reply = msg.Reply

		wm.mu.Lock()
		wm.clock.Update(req.Timestamp)
		wm.Enqueue(&req)
		wm.mu.Unlock()

	}); err != nil {
		log.Fatalf("FATAL: WM failed to subscribe to %s: %v", SubjectWaterConnect, err)
	}

	if err := wm.bus.Subscribe(SubjectWaterDisconnect, func(msg *nats.Msg) {
		var req ConnectionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			log.Printf("WM Error unmarshaling disconnect request: %v", err)
			return
		}

		wm.mu.Lock()
		wm.clock.Update(req.Timestamp)
		if wm.connections > 0 {
			wm.connections--
		}
		wm.mu.Unlock()

		wm.SendStatusRespone(msg, req.TruckID, false)
	}); err != nil {
		log.Fatalf("FATAL: WM failed to subscribe to %s: %v", SubjectWaterDisconnect, err)
	}
}

func (wm *WaterManager) ProcessQueue() {
	for {
		wm.mu.Lock()
		req := wm.Dequeue()

		if req == nil {
			wm.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}

		newConsumption := (wm.connections + 1) * wm.consumptionRate
		canConnect := wm.volume >= newConsumption

		if canConnect {
			wm.connections++
			// fmt.Printf("✅ Manager processed CONNECT for Truck %d. Active: %d (Volume: %d)\n", req.TruckID, wm.connections, wm.volume)
		} else {
			// fmt.Printf("❌ Manager DENIED CONNECT for Truck %d. Active: %d (Volume: %d - NO WATER)\n", req.TruckID, wm.connections, wm.volume)
		}

		wm.mu.Unlock()

		if req.Reply != "" {
			resp := WaterStatusResponse{TruckID: req.TruckID, Status: canConnect}
			data, _ := json.Marshal(resp)

			wm.bus.Publish(req.Reply, data)
		}
	}
}

func (wm *WaterManager) SendStatusRespone(msg *nats.Msg, truckID int, status bool) {
	if msg.Reply == "" {
		log.Printf("WM WARNING: Message from Truck %d has no reply subject.", truckID)
		return
	}

	resp := WaterStatusResponse{TruckID: truckID, Status: status}
	data, _ := json.Marshal(resp)

	if err := msg.Respond(data); err != nil {
		log.Printf("WM Error responding to Truck %d: %v", truckID, err)
	}
}

func (wm *WaterManager) RefillWaterSupply(done <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.mu.Lock()

			if wm.connections > 0 {
				drain := wm.connections * wm.consumptionRate
				wm.volume -= drain

				if wm.volume < 0 {
					wm.volume = 0
				}
			}

			if wm.volume < MaxVolume {
				newVolume := wm.volume + wm.refillRate
				wm.volume = min(newVolume, MaxVolume)
			}

			wm.mu.Unlock()

		case <-done:
			return
		}
	}
}
