package flow

import (
	"context"
	"fmt"
	"github.com/bytepowered/assert-go"
)

type MultipleRunner struct {
	input     Input
	pipelines []*Pipeline
}

func NewMultipleRunner() *MultipleRunner {
	return &MultipleRunner{}
}

func (r *MultipleRunner) AddPipeline(v *Pipeline) *MultipleRunner {
	assert.MustNotNil(v, "MultipleRunner add a nil pipeline")
	if r.input == nil {
		r.input = v.input
	}
	assert.MustTrue(r.input == v.input, "MultipleRunner requires the SAME pipeline, exists: %T, adding: %T", r.input, v.input)
	r.pipelines = append(r.pipelines, v)
	return r
}

func (r *MultipleRunner) Serve(ctx context.Context) error {
	assert.MustTrue(len(r.pipelines) > 0, "MultipleRunner requires pipelines")
	type deliverer struct {
		fun DeliverFunc
		id  string
	}
	var delivers []deliverer
	for _, pipe := range r.pipelines {
		delivers = append(delivers, deliverer{
			fun: pipe.newDeliverFunc(ctx),
			id:  pipe.Id,
		})
	}
	return r.input.OnRead(ctx, func(msg Message) error {
		for _, deliver := range delivers {
			if err := deliver.fun(msg); err != nil {
				return fmt.Errorf("MultipleRunner deliver error, pipeline: %s, %w", deliver.id, err)
			}
		}
		return nil
	})
}
