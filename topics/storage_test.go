package topics

import (
	"testing"

	"github.com/mailru/surgemq/message"
)

const (
	testDbName = "test_db"
)

func newMessage(topic, payload string) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetTopic([]byte(topic))
	msg.SetPayload([]byte(payload))
	return msg
}

func areMessagesEqual(a *message.PublishMessage, b *message.PublishMessage) bool {
	areTopicsEq := string(a.Topic()) == string(b.Topic())
	arePayloadsEq := string(a.Payload()) == string(b.Payload())
	return areTopicsEq && arePayloadsEq
}

func TestStorageHasDataAfterStop(t *testing.T) {
	storage, err := NewPudgeStorage(testDbName, DefaultSyncIntv)
	if err != nil {
		t.Error(err.Error())
	}

	aMsg := newMessage("foo/aaa", "aaa")
	if err = storage.insert(aMsg.Topic(), aMsg); err != nil {
		t.Error(err.Error())
	}

	bMsg := newMessage("foo/bbb", "bbb")
	if err = storage.insert(bMsg.Topic(), bMsg); err != nil {
		t.Error(err.Error())
	}

	cMsg := newMessage("foo/ccc", "ccc")
	if err = storage.insert(cMsg.Topic(), cMsg); err != nil {
		t.Error(err.Error())
	}

	if err = storage.remove(bMsg.Topic()); err != nil {
		t.Error(err.Error())
	}

	if err = storage.close(); err != nil {
		t.Error(err.Error())
	}

	storage, err = NewPudgeStorage(testDbName, DefaultSyncIntv)
	if err != nil {
		t.Error(err.Error())
	}

	defer storage.clear()

	messages, err := storage.messages()
	if err != nil {
		t.Error(err.Error())
	}

	const r = 2
	if n := len(messages); n != r {
		t.Errorf("number of messages remained is %d != %d", n, r)
	}

	if !areMessagesEqual(messages[0], aMsg) {
		t.Errorf(
			"unexpected message with topic '%s' and payload '%s'",
			messages[0].Topic(),
			messages[0].Payload(),
		)
	}

	if !areMessagesEqual(messages[1], cMsg) {
		t.Errorf(
			"unexpected message with topic '%s' and payload '%s'",
			messages[1].Topic(),
			messages[1].Payload(),
		)
	}
}
