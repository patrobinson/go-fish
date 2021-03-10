# Go Fish

Go Fish is a stream processor built in Golang. It provides the capability to write rules in Go as Go Plugins, which are dynamically linked at runtime.

Go Fish seeks to implement similar functionality to Apache Samza, without tight coupling to Kafka or YARN. Currently it implements reading streams, writing output events and state management. Future versions may include Checkpointing, Windowing, Metrics and a cluster implementation to co-ordinate message routing between nodes.


**Development Status**: No longer in active development. [Benthos](https://github.com/Jeffail/benthos) is an excellent alternative.


## Project Goals

Go Fish is designed to provide a lower barrier of entry for Stream Processing than the Apache family of frameworks. The existing frameworks are predominantly written in Java and require at the very lease ZooKeeper to run, but usually Kafka and potentially Hadoop.

While projects like [Wallaroo](https://github.com/wallaroolabs/wallaroo) exist that provide native Go integration and are easier to get started it only supports Kafka and TCP as an input source. Go Fish aims to leverage existing cloud tooling like Kinesis and DynamoDB to provide easier setup and maintenance. Support for other cloud providers may be added in the future.

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

A trivial pipeline can be defined in a json file and run like so:

```
go-fish -pipelineConfig pipeline.json
```

More realisticly you will want to run multiple pipelines. To do this create an API Configuration file and submit your pipelines via the API.

```json
{
  "listenAddress": "127.0.0.1:8000",
  "backendConfig": {
    "type": "boltdb|dynamodb",
    "boldDBConfig": {
      "bucketName": "go-fish",
      "databaseName": "go"
    },
    "dynamoDBConfig": {
      "region": "us-east-2",
      "tableName": "go-fish"
    }
  }
}
```

The DynamoDB Schema must be a Primary Key of "UUID" which is a binary blob and no Hash Key.

```
go-fish -apiConfig api.json
```

### Examples

See `examples/` for some implementations of go-fish. You can with the following command:
```
make certstream-example
```

### Defining a Pipeline

A Pipeline contains five top level keys, `eventFolder`, `rules`, `states`, `sources` and `sinks`. In a rule definition the source, state and sink should refer to a key in the respective top level.

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

This Pipeline would create a Directed Acyclical Graph like so, although more complex topologies are possible:

```
fileInput ----> searchRule ----> conversionRule ----> fileOutput
```

#### Creating an Event Struct

The Event Struct simply defines the data structure for the event and implements the `event` interface. This is a trivial example where the event contains just a single string:

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

#### Writing an Event Type

The Event Type is responsible for decoding the Event. It implements the `eventType` interface.

```
type eventType interface {
	Name() string
	Decode([]byte) (event.Event, error)
}
```

Where event is a minimal package that can be imported from `github.com/patrobinson/go-fish/event`.

#### Writing a Rule

The Rule must also include the Event struct, so that it can assert the Event it receives is the Event Struct it's expecting.
It implements the Rule interface.

```
type Rule interface {
	Init(state ...interface{}) error
	Process(interface{}) interface{}
	String() string
	WindowInterval() int
	Window() ([]output.OutputEvent, error)
	Close() error
}
```

Simple rules that do not require state or windowing can utilise the BasicRule helper. This implements everything except for the `Process` and `String` methods.

```
import "github.com/patrobinson/go-fish/ruleHelpers"

type simpleRule struct {
	rulehelpers.BasicRule
}
```
