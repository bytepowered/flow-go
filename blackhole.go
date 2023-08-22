package flow

import (
	"context"
)

var (
	_ Output = (*BlackHoleOutput)(nil)
)

type BlackHoleOutput struct {
}

func NewBlackHoleOutput() Output {
	return &BlackHoleOutput{}
}

func (b *BlackHoleOutput) OnSend(ctx context.Context, events ...Event) {
	// nop
}
