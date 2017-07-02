package eventStructs

const EventName = "exampleEventType"

type ExampleType struct {
	Str string
}

func (e ExampleType) TypeName() string {
	return EventName
}
