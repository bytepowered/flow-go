package flow

import (
	"context"
	"fmt"
	"github.com/bytepowered/assert-go"
)

type PipelineRunner struct {
	pipeline Pipeline
}

func NewPipelineRunner(pipeline Pipeline) *PipelineRunner {
	return &PipelineRunner{
		pipeline: pipeline,
	}
}

func (p *PipelineRunner) Id() string {
	return p.pipeline.Id
}

func (p *PipelineRunner) Serve(ctx context.Context) error {
	assert.MustTrue(len(p.pipeline.outputs) > 0, "Pipeline "+p.pipeline.Id+" output is empty")
	return p.pipeline.input.OnRead(ctx, func(event Event) (rerr error) {
		defer func() {
			if r := recover(); r != nil {
				if re, ok := r.(error); ok {
					rerr = re
				} else {
					rerr = fmt.Errorf("Pipeline "+p.pipeline.Id+" error: %s", r)
				}
			}
		}()
		// Filter -> Transform -> Output
		ferr := p.makeFilterChain(func(ctx context.Context, in Event) (perr error) {
			if in == nil {
				return nil
			}
			events := []Event{in}
			for _, tf := range p.pipeline.transformers {
				events, perr = tf.DoTransform(ctx, events)
				if perr != nil {
					return perr
				}
				if len(events) == 0 {
					return nil
				}
			}
			for _, output := range p.pipeline.outputs {
				output.OnSend(ctx, events...)
			}
			return nil
		}, p.pipeline.filters)(ctx, event)
		if ferr != nil {
			return fmt.Errorf("Pipeline "+p.pipeline.Id+" serve error: %w", ferr)
		}
		return nil
	})
}

func (p *PipelineRunner) makeFilterChain(next FilterFunc, filters []Filter) FilterFunc {
	for i := len(filters) - 1; i >= 0; i-- {
		next = filters[i].DoFilter(next)
	}
	return next
}
