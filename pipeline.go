package flow

import "github.com/bytepowered/assert-go"

type Pipeline struct {
	Id           string
	input        Input
	filters      []Filter
	transformers []Transformer
	outputs      []Output
}

func NewPipeline(id string, input Input) *Pipeline {
	assert.MustNotEmpty(id, "Pipeline id is empty")
	assert.MustNotNil(input, "Pipeline "+id+" input is nil")
	return &Pipeline{
		Id:           id,
		input:        input,
		filters:      make([]Filter, 0, 4),
		transformers: make([]Transformer, 0, 4),
		outputs:      make([]Output, 0, 4),
	}
}

func (p *Pipeline) AddFilter(f Filter) {
	p.filters = append(p.filters, f)
}

func (p *Pipeline) AddTransformer(t Transformer) {
	p.transformers = append(p.transformers, t)
}

func (p *Pipeline) AddOutput(d Output) {
	p.outputs = append(p.outputs, d)
}
