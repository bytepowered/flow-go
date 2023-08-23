package flow

import (
	"context"
	"github.com/bytepowered/assert-go"
)

type SingleRunner struct {
	inputRef Input
	pipeline *Pipeline
}

func NewSingleRunner(input Input) *SingleRunner {
	assert.MustNotNil(input, "Input is nil")
	return &SingleRunner{
		inputRef: input,
	}
}

func (r *SingleRunner) SetPipeline(v *Pipeline) *SingleRunner {
	assert.MustNotNil(v, "Pipeline is nil")
	return r
}

func (r *SingleRunner) Serve(ctx context.Context) error {
	return r.inputRef.OnRead(ctx, r.pipeline.buildDeliverFunc(ctx))
}
