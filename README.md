# Go Fish

Go Fish is a stream processor built in Golang. It provides the capability to write rules in Go as Go Plugins, which are dynamically linked at runtime.

Go Fish seeks to implement similar functionality to Apache Samza, without tight coupling to Kafka or YARN. Currently it implements reading streams, writing output events and state management. Future versions may include Checkpointing, Windowing, Metrics and a cluster implementation to co-ordinate message routing between nodes.


**Development Status**: Ready for alpha testing

## Bare minimum

The bare minimum to have a working example requires you to create a Rule and an Event Type.
These must be compiled as plugins and the path to the compiled plugins supplied as an argument to go-fish.

To avoid duplication, the event struct can be extracted to a seperate file, as the struct is required by both the Rule and Event Type.
A repository of Rules, Event structs and Event Types may look like so:

```
.
├── build.sh
├── eventStructs
│   └── struct.go
├── eventTypes
│   ├── event_type.go
└── rules
    ├── rule.go

```

Where `build.sh` does the following:

```
for DIR in "eventTypes rules"; do
    for FILE in $(ls $DIR); do
        go build -buildmode=plugin -o ${DIR}/${FILE%.go}.so ${DIR}/${FILE}
    done
done
```

### Usage

go-fish -config pipeline.json

### Examples

See `examples/` for some implementations of go-fish. You can with the following command:
```
make certstream-example
```

### Defining a Pipeline

A Pipeline contains four top level keys, `eventFolder`, `rules`, `states`, `sources` and `sinks`. In a rule definition the source, state and sink should refer to a key in the respective top level.

```json
{
  "eventFolder": "testdata/eventTypes",
  "rules": {
    "searchRule": {
      "source": "fileInput",
      "state": "searchConversion",
      "plugin": "rules/searchRule.so",
      "sink": "conversionRule"
    },
    "conversionRule": {
      "source": "searchRule",
      "plugin": "rules/conversionRule.so",
      "sink": "fileOutput"
    }
  },
  "states": {
    "searchConversion": {
      "type": "KV"
    }
  },
  "sources": {
    "fileInput": {
      "type": "File",
      "file_config": {
        "path": "testdata/pipelines/input"
      }
    }
  },
  "sinks": {
    "fileOutput": {
      "type": "File",
      "file_config": {
        "path": "testdata/output"
      }
    }
  }
}
```

This Pipeline would create a Directed Acyclical Graph like so:

```
fileInput ----> searchRule ----> conversionRule ----> fileOutput
```

### Creating an Event Struct

The Event Struct can simply defines the data structure for the event and implements the `event` interface. This is a trivial example where the event contains just a single string:

```
package eventStructs

const EventName = "exampleEventType"

type ExampleType struct {
	Str string
}

func (e ExampleType) TypeName() string {
	return EventName
}
```

In this example we export the `EventName` variable, which can be re-used in the Event Type to implement the `Name()` method.

### Writing an Event Type

The Event Type is responsible for decoding the Event. It implements the `eventType` interface.

```
type eventType interface {
	Name() string
	Decode([]byte) (event.Event, error)
}
```

Where event is a minimal package that can be imported from `github.com/patrobinson/go-fish/event`.

### Writing a Rule

The Rule must also include the Event struct, so that it can assert the Event it receives is the Event Struct it's expecting.
It implements the Rule interface.

```
type Rule interface {
	Process(interface{}) bool
	String() string
}
```
