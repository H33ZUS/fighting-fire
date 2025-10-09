package main

const (
	SubjectWaterConnect    = "water.request.connect"
	SubjectWaterDisconnect = "water.request.disconnect"
)

type ConnectionRequest struct {
	TruckID   int `json:"id"`
	Timestamp int `json:"timestamp"`
	Reply     string
}

type WaterStatusResponse struct {
	TruckID int  `json:"id"`
	Status  bool `json:"status"`
}
