package flow

import (
	"context"
	"fmt"
	"github.com/bytepowered/assert-go"
)

type MultipleRunner struct {
	inputRef  Input
	pipelines []*Pipeline
}

func NewMultipleRunner(input Input) *MultipleRunner {
	assert.MustNotNil(input, "Input is nil")
	return &MultipleRunner{
		inputRef: input,
	}
}

func (r *MultipleRunner) AddPipeline(v *Pipeline) *MultipleRunner {
	assert.MustNotNil(v, "Pipeline is nil")
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
			fun: pipe.buildDeliverFunc(ctx),
			id:  pipe.Id,
		})
	}
	return r.inputRef.OnRead(ctx, func(msg Message) error {
		for _, deliver := range delivers {
			if err := deliver.fun(msg); err != nil {
				return fmt.Errorf("MultipleRunner deliver error, pipeline: %s, %w", deliver.id, err)
			}
		}
		return nil
	})
}
