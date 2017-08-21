package rule_helpers

/*
BasicRule implements the Init(), WindowInterval(), Window() and Close() functions.
It is useful for stateless Rules that don't need to implement them.
It can be added to your basic rule like so

	type MyBasicRule struct {
		rule_helpers.BasicRule
	}
*/
type BasicRule struct{}

func (b *BasicRule) Init() {}

func (b *BasicRule) WindowInterval() int {
	return 0
}

func (b *BasicRule) Window() error {
	return nil
}

func (b *BasicRule) Close() {}
