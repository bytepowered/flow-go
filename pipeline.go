package flow

import (
	"context"
	"fmt"
	"github.com/bytepowered/assert-go"
)

type Pipeline struct {
	Id           string
	filters      []Filter
	transformers []Transformer
	outputs      []Output
	router       Router
	build        bool
}

func NewPipeline(id string) *Pipeline {
	assert.MustNotEmpty(id, "Pipeline id is empty")
	return &Pipeline{
		Id:           id,
		filters:      make([]Filter, 0, 4),
		transformers: make([]Transformer, 0, 4),
		outputs:      make([]Output, 0, 4),
		build:        false,
	}
}

func (p *Pipeline) AddFilter(v Filter) *Pipeline {
	assert.MustFalse(p.build, "Pipeline already in used")
	assert.MustNotNil(v, "Filter is nil")
	p.filters = append(p.filters, v)
	return p
}

func (p *Pipeline) AddTransformer(v Transformer) *Pipeline {
	assert.MustFalse(p.build, "Pipeline already in used")
	assert.MustNotNil(v, "Transformer is nil")
	p.transformers = append(p.transformers, v)
	return p
}

func (p *Pipeline) AddOutput(v Output) *Pipeline {
	assert.MustFalse(p.build, "Pipeline already in used")
	assert.MustNotNil(v, "Output is nil")
	p.outputs = append(p.outputs, v)
	return p
}

func (p *Pipeline) SetRouter(v Router) *Pipeline {
	assert.MustFalse(p.build, "Pipeline already in used")
	assert.MustNotNil(v, "Router is nil")
	p.router = v
	return p
}

func (p *Pipeline) buildDeliverFunc(ctx context.Context) DeliverFunc {
	p.build = true
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
		// [Filters] -> [Transformers] -> [Router] -> Output
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
			if p.router != nil {
				// Route message
				for _, msg := range messages {
					for _, output := range p.router.DoRoute(ctx, msg, p.outputs) {
						output.OnSend(ctx, msg)
					}
				}
			} else {
				// Send direct
				for _, output := range p.outputs {
					output.OnSend(ctx, messages...)
				}
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
