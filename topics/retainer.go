package topics

import (
	"github.com/mailru/surgemq/message"
)

type Retainer interface {
	Insert(topic []byte, msg *message.PublishMessage) error
	Remove(topic []byte) error
	Match(topic []byte, msgs *[]*message.PublishMessage) error
	Close() error
}

type StorageRetainer struct {
	store    Storage
	retainer *MemRetainer
}

func NewStorageRetainer(s Storage) (*StorageRetainer, error) {
	r := &StorageRetainer{
		store:    s,
		retainer: NewMemRetainer(),
	}

	messages, err := s.messages()
	if err != nil {
		return nil, err
	}

	for _, msg := range messages {
		err = r.retainer.Insert(msg.Topic(), msg)
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (s *StorageRetainer) Insert(topic []byte, msg *message.PublishMessage) error {
	err := s.retainer.Insert(topic, msg)
	if err != nil {
		return err
	}

	return s.store.insert(topic, msg)
}

func (s *StorageRetainer) Remove(topic []byte) error {
	err := s.retainer.Remove(topic)
	if err != nil {
		return err
	}

	return s.store.remove(topic)
}

func (s *StorageRetainer) Match(topic []byte, msgs *[]*message.PublishMessage) error {
	return s.retainer.Match(topic, msgs)
}

func (s *StorageRetainer) Close() error {
	err := s.retainer.Close()
	if err != nil {
		return err
	}

	return s.store.close()
}

type MemRetainer struct {
	root *rnode
}

func NewMemRetainer() *MemRetainer {
	return &MemRetainer{
		root: newRNode(),
	}
}

func (m *MemRetainer) Insert(topic []byte, msg *message.PublishMessage) error {
	return m.root.rinsert(topic, msg)
}

func (m *MemRetainer) Remove(topic []byte) error {
	return m.root.rremove(topic)
}

func (m *MemRetainer) Match(topic []byte, msgs *[]*message.PublishMessage) error {
	return m.root.rmatch(topic, msgs)
}

func (m *MemRetainer) Close() error {
	m.root = nil
	return nil
}
