package main

const (
	SubjectWaterConnect    = "water.request.connect"
	SubjectWaterDisconnect = "water.request.disconnect"

	SubjectNewFires  = "fire.new"
	SubjectExFires   = "fire.ex"
	SubjectClaimFire = "fire.claim"
)

type ConnectionRequest struct {
	TruckID   int `json:"id"`
	Timestamp int `json:"timestamp"`
	Reply     string
	WaitChan  chan bool `json:"-"`
}

type WaterStatusResponse struct {
	TruckID int  `json:"id"`
	Status  bool `json:"status"`
}

type FireEvent struct {
	X               int `json:"x"`
	Y               int `json:"y"`
	Timestamp       int `json:"timestamp"`
	TruckID         int `json:"id,omitempty"`
	ClaimingTruckID int `json:"claiming_truck_id"`
}
