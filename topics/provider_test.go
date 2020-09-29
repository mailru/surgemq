package topics

import (
	"testing"

	"github.com/mailru/surgemq/message"
	"github.com/recoilme/pudge"
	"github.com/stretchr/testify/require"
)

type TestSubscriber struct{}

func (s *TestSubscriber) OnPublish(msg *message.PublishMessage) error { return nil }

func newPublishMessageLarge(topic []byte, qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetTopic(topic)
	msg.SetPayload(make([]byte, 1024))
	msg.SetQoS(qos)

	return msg
}

func TestMemTopicsSubscription(t *testing.T) {
	Unregister("mem")
	p := NewMemProvider()
	Register("mem", p)

	mgr, err := NewManager("mem")

	MaxQosAllowed = 1
	subscriber := &TestSubscriber{}
	qos, err := mgr.Subscribe([]byte("sports/tennis/+/stats"), 2, subscriber, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, int(qos))

	err = mgr.Unsubscribe([]byte("sports/tennis"), subscriber, struct{}{})

	require.Error(t, err)

	subs := make([]Subscriber, 5)
	qoss := make([]byte, 5)

	err = mgr.Subscribers([]byte("sports/tennis/anzel/stats"), 2, &subs, &qoss, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 0, len(subs))

	err = mgr.Subscribers([]byte("sports/tennis/anzel/stats"), 1, &subs, &qoss, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, 1, int(qoss[0]))

	err = mgr.Unsubscribe([]byte("sports/tennis/+/stats"), subscriber, struct{}{})

	require.NoError(t, err)
}

func TestMemTopicsRetained(t *testing.T) {
	Unregister("mem")
	p := NewMemProvider()
	Register("mem", p)

	mgr, err := NewManager("mem")
	require.NoError(t, err)
	require.NotNil(t, mgr)

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err = mgr.Retain(msg1, struct{}{})
	require.NoError(t, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = mgr.Retain(msg2, struct{}{})
	require.NoError(t, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = mgr.Retain(msg3, struct{}{})
	require.NoError(t, err)

	var msglist []*message.PublishMessage

	// ---

	err = mgr.Retained(msg1.Topic(), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg2.Topic(), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg3.Topic(), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/+"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/#"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/+/stats"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/#"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 3, len(msglist))
}

func TestPudgeTopicsRetained(t *testing.T) {
	const dbName = "test_db"
	Unregister("pudge")
	p, err := NewPudgeProvider(dbName, 1)
	require.NoError(t, err)
	Register("pudge", p)

	mgr, err := NewManager("pudge")
	require.NoError(t, err)
	require.NotNil(t, mgr)

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err = mgr.Retain(msg1, struct{}{})
	require.NoError(t, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = mgr.Retain(msg2, struct{}{})
	require.NoError(t, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = mgr.Retain(msg3, struct{}{})
	require.NoError(t, err)

	mgr.Close()

	Unregister("pudge")
	p, err = NewPudgeProvider("test_db", 1)
	require.NoError(t, err)
	Register("pudge", p)

	mgr, err = NewManager("pudge")
	require.NoError(t, err)
	require.NotNil(t, mgr)

	var msglist []*message.PublishMessage

	err = mgr.Retained(msg1.Topic(), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg2.Topic(), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg3.Topic(), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/+"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/#"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/+/stats"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/#"), &msglist, struct{}{})

	require.NoError(t, err)
	require.Equal(t, 3, len(msglist))

	pudge.DeleteFile(dbName)
}
