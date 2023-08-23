package flow

import (
	"context"
	"fmt"
	"github.com/bytepowered/assert-go"
)

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

func (p *Pipeline) newDeliverFunc(ctx context.Context) DeliverFunc {
	return func(msg Message) (rerr error) {
		defer func() {
			if rec := recover(); rec != nil {
				if re, ok := rec.(error); ok {
					rerr = re
				} else {
					rerr = fmt.Errorf("Pipeline "+p.Id+" crash: %s", rec)
				}
			}
		}()
		// Filter -> Transform -> Output
		ferr := makeFilterChain(func(ctx context.Context, in Message) (perr error) {
			if in == nil {
				return nil
			}
			messages := []Message{in}
			for _, tf := range p.transformers {
				messages, perr = tf.DoTransform(ctx, messages)
				if perr != nil {
					return perr
				}
				if len(messages) == 0 {
					return nil
				}
			}
			for _, output := range p.outputs {
				output.OnSend(ctx, messages...)
			}
			return nil
		}, p.filters)(ctx, msg)
		if ferr != nil {
			return fmt.Errorf("Pipeline "+p.Id+" deliver error: %w", ferr)
		}
		return nil
	}
}

func makeFilterChain(next FilterFunc, filters []Filter) FilterFunc {
	for i := len(filters) - 1; i >= 0; i-- {
		next = filters[i].DoFilter(next)
	}
	return next
}
