package flow

import (
	"context"
	"fmt"
	"github.com/bytepowered/assert-go"
)

type Runner struct {
	pipeline *Pipeline
}

func NewRunner(pipeline *Pipeline) *Runner {
	return &Runner{
		pipeline: pipeline,
	}
}

func (r *Runner) Id() string {
	return r.pipeline.Id
}

func (r *Runner) Serve(ctx context.Context) error {
	assert.MustTrue(len(r.pipeline.outputs) > 0, "Runner "+r.pipeline.Id+" output is empty")
	return r.pipeline.input.OnRead(ctx, func(event Event) (rerr error) {
		defer func() {
			if rec := recover(); rec != nil {
				if re, ok := rec.(error); ok {
					rerr = re
				} else {
					rerr = fmt.Errorf("Runner "+r.pipeline.Id+" error: %s", rec)
				}
			}
		}()
		// Filter -> Transform -> Output
		ferr := r.makeFilterChain(func(ctx context.Context, in Event) (perr error) {
			if in == nil {
				return nil
			}
			events := []Event{in}
			for _, tf := range r.pipeline.transformers {
				events, perr = tf.DoTransform(ctx, events)
				if perr != nil {
					return perr
				}
				if len(events) == 0 {
					return nil
				}
			}
			for _, output := range r.pipeline.outputs {
				output.OnSend(ctx, events...)
			}
			return nil
		}, r.pipeline.filters)(ctx, event)
		if ferr != nil {
			return fmt.Errorf("Runner "+r.pipeline.Id+" serve error: %w", ferr)
		}
		return nil
	})
}

func (r *Runner) makeFilterChain(next FilterFunc, filters []Filter) FilterFunc {
	for i := len(filters) - 1; i >= 0; i-- {
		next = filters[i].DoFilter(next)
	}
	return next
}
