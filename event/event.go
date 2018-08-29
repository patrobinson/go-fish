package event

// Event is the interface event structures must implement
type Event interface {
	TypeName() string
}
