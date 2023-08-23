package flow

import (
	"context"
	"fmt"
	"time"
)

var (
	_kindNameMapper = make(KindNameMapper, 8)
)

// Kind 表示消息类型
type Kind uint16

func (k Kind) String() string {
	return _kindNameMapper.GetName(k)
}

var _ fmt.Stringer = new(Header)

// Header 消息Header
type Header struct {
	Unix int64  `json:"eunixms"` // 用于标识发生消息的时间戳，精确到毫秒
	Tag  string `json:"etag"`    // 用于标识发生消息来源的标签，通常格式为: origin.vendor
	Kind Kind   `json:"ekind"`   // 消息类型，由业务定义
	Id   int64  `json:"eid"`     // 消息ID
}

func (h Header) String() string {
	return fmt.Sprintf(`timestamp: %s, tag: %s, kind: %s`,
		time.UnixMilli(h.Unix), h.Tag, h.Kind)
}

type Message interface {
	// Tag 返回消息标签。与 Header.Tag 一致。
	Tag() string
	// Kind 返回消息类型。与 Header.Kind 一致。
	Kind() Kind
	// Time 返回消息发生时间。与 Header.Unix 精度一致。
	Time() time.Time
	// ID 返回消息ID，与 Header.Id 一致。
	ID() int64
	// Header 返回消息Header
	Header() Header
	// Data 返回消息记录对象
	Data() interface{}
}

type DeliverFunc func(msg Message) error

// Input 消息输入源
type Input interface {
	OnRead(ctx context.Context, deliverer DeliverFunc) error
}

// Output 消息输出源
type Output interface {
	OnSend(ctx context.Context, messages ...Message)
}

// FilterFunc 执行过滤原始消息的函数；
type FilterFunc func(ctx context.Context, msg Message) error

// Filter 原始消息过滤接口
type Filter interface {
	DoFilter(next FilterFunc) FilterFunc
}

// Transformer 处理消息格式转换
type Transformer interface {
	DoTransform(ctx context.Context, in []Message) (out []Message, err error)
}
