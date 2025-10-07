package helper

func RemoveDirection(directions []int, value int) []int {
	for i, v := range directions {
		if v == value {
			return append(directions[:i], directions[i+1:]...)
		}
	}
	return directions
}
