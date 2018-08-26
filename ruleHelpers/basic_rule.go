package rule_helpers

import (
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
)

/*
BasicRule implements the Init(), WindowInterval(), Window() and Close() functions.
It is useful for stateless Rules that don't need to implement them.
It can be added to your basic rule like so

	type MyBasicRule struct {
		rule_helpers.BasicRule
	}
*/
type BasicRule struct{}

func (b *BasicRule) Init() error {
	return nil
}

func (b *BasicRule) SetState(_ state.State) error {
	return nil
}

func (b *BasicRule) WindowInterval() int {
	return 0
}

func (b *BasicRule) Window() ([]output.OutputEvent, error) {
	return []output.OutputEvent{}, nil
}

func (b *BasicRule) Close() error {
	return nil
}
