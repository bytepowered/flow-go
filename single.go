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

func (r *SingleRunner) SetPipeline(v *Pipeline) *SingleRunner {
	assert.MustNotNil(v, "SingleRunner set a nil pipeline")
	return r
}

func (r *SingleRunner) Serve(ctx context.Context) error {
	return r.single.input.OnRead(ctx, r.single.newDeliverFunc(ctx))
}
