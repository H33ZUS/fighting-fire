package main

const (
	SubjectWaterConnect    = "water.request.connect"
	SubjectWaterDisconnect = "water.request.disconnect"
)

type ConnectionRequest struct {
	TruckID int `json:"id"`
}
