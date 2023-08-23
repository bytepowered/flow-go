package flow

import (
	"bytes"
	"time"
)

var _ Message = NewObjectMessage(Header{}, nil)

type ObjectMessage struct {
	Headers   Header
	data      interface{}
	timestamp time.Time
}

func NewObjectMessage(header Header, data interface{}) *ObjectMessage {
	return &ObjectMessage{
		Headers:   header,
		data:      data,
		timestamp: time.UnixMilli(header.Unix),
	}
}

func (m *ObjectMessage) ID() int64 {
	return m.Headers.Id
}

func (m *ObjectMessage) Tag() string {
	return m.Headers.Tag
}

func (m *ObjectMessage) Kind() Kind {
	return m.Headers.Kind
}

func (m *ObjectMessage) Time() time.Time {
	return m.timestamp
}

func (m *ObjectMessage) Header() Header {
	return m.Headers
}

func (m *ObjectMessage) Data() interface{} {
	return m.data
}

//// Bytes

var _ Message = NewFrameMessage(Header{}, nil)

type FrameMessage struct {
	*ObjectMessage
}

func NewFrameMessage(header Header, frame []byte) *FrameMessage {
	return &FrameMessage{
		ObjectMessage: NewObjectMessage(header, frame),
	}
}

func (e *FrameMessage) Bytes() []byte {
	return e.Data().([]byte)
}

//// Text

var _ Message = NewTextMessage(Header{}, "")

type TextFrame struct {
	*ObjectMessage
}

func NewTextMessage(header Header, text string) *TextFrame {
	return &TextFrame{
		ObjectMessage: NewObjectMessage(header, text),
	}
}

func (e *TextFrame) Text() string {
	return e.Data().(string)
}

//// Fields

var _ Message = NewFieldsMessage(Header{}, nil)

type FieldsMessage struct {
	*ObjectMessage
}

func NewFieldsMessage(header Header, fields []string) *FieldsMessage {
	return &FieldsMessage{
		ObjectMessage: NewObjectMessage(header, fields),
	}
}

func (e *FieldsMessage) Fields() []string {
	return e.Data().([]string)
}

//// Buffer

var _ Message = NewBufferMessage(Header{}, nil)

type BufferMessage struct {
	*ObjectMessage
}

func NewBufferMessage(header Header, buffer *bytes.Buffer) *BufferMessage {
	return &BufferMessage{
		ObjectMessage: NewObjectMessage(header, buffer),
	}
}

func (e *BufferMessage) Buffer() *bytes.Buffer {
	return e.Data().(*bytes.Buffer)
}
