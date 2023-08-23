package flow

import (
	"context"
	"fmt"
	"github.com/bytepowered/assert-go"
)

type MultiRunner struct {
	input    Input
	multiple []*Pipeline
}

func NewMultiRunner() *MultiRunner {
	return &MultiRunner{}
}

func (r *MultiRunner) AddPipeline(pipeline *Pipeline) *MultiRunner {
	assert.MustNotNil(pipeline, "MultiRunner add a nil pipeline")
	if r.input == nil {
		r.input = pipeline.input
	}
	assert.MustTrue(r.input == pipeline.input, "MultiRunner requires the SAME pipeline, exists: %T, adding: %T", r.input, pipeline.input)
	r.multiple = append(r.multiple, pipeline)
	return r
}

func (r *MultiRunner) Serve(ctx context.Context) error {
	assert.MustNotNil(len(r.multiple) > 0, "MultiRunner requires pipelines")
	var delivers []DeliverFunc
	for _, pipe := range r.multiple {
		delivers = append(delivers, pipe.newDeliverFunc(ctx))
	}
	return r.input.OnRead(ctx, func(event Event) error {
		for _, deliver := range delivers {
			if err := deliver(event); err != nil {
				return fmt.Errorf("MultiRunner deliver error: %w", err)
			}
		}
		return nil
	})
}
