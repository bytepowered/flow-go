package flow

import (
	"context"
	"github.com/bytepowered/assert-go"
)

type SingleRunner struct {
	single *Pipeline
}

func NewSingleRunner() *SingleRunner {
	return &SingleRunner{}
}

func (r *SingleRunner) SetPipeline(pipeline *Pipeline) *SingleRunner {
	assert.MustNotNil(pipeline, "SingleRunner set a nil pipeline")
	return r
}

func (r *SingleRunner) Serve(ctx context.Context) error {
	return r.single.input.OnRead(ctx, r.single.newDeliverFunc(ctx))
}
