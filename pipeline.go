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

func (p *Pipeline) AddFilter(v Filter) {
	assert.MustNotNil(v, "Filter is nil")
	p.filters = append(p.filters, v)
}

func (p *Pipeline) AddTransformer(v Transformer) {
	assert.MustNotNil(v, "Transformer is nil")
	p.transformers = append(p.transformers, v)
}

func (p *Pipeline) AddOutput(v Output) {
	assert.MustNotNil(v, "Output is nil")
	p.outputs = append(p.outputs, v)
}
