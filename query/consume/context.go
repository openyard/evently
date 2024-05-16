package consume

import "context"

type Context struct {
	ctx            context.Context
	cancel         context.CancelFunc
	messages       []Msg
	subscriptionID string
}

func NewContext(subscriptionID string, messages []Msg) Context {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return Context{
		ctx:            ctx,
		cancel:         cancelFunc,
		messages:       messages,
		subscriptionID: subscriptionID,
	}
}

func (c Context) Context() (context.Context, context.CancelFunc) {
	return c.ctx, c.cancel
}

func (c Context) SubscriptionID() string {
	return c.subscriptionID
}

func (c Context) WithParent(ctx context.Context) Context {
	child := NewContext(c.subscriptionID, c.messages)
	child.ctx = ctx
	return child
}

type Msg struct {
	msgID   string
	msgType string
	msg     []byte
}

func NewMsg(msgID, msgType string, msg []byte) Msg {
	return Msg{
		msgID:   msgID,
		msgType: msgType,
		msg:     msg,
	}
}
