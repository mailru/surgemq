package topics

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailru/surgemq/message"
)

var (
	// MaxQosAllowed is the maximum QOS supported by this server
	MaxQosAllowed = message.QosExactlyOnce
)

type Provider struct {
	rmut     *sync.RWMutex
	retainer Retainer
	smut     *sync.RWMutex
	sessions SessionKeeper
}

func NewProvider(r Retainer, s SessionKeeper) *Provider {
	return &Provider{
		rmut:     &sync.RWMutex{},
		retainer: r,
		smut:     &sync.RWMutex{},
		sessions: s,
	}
}

func (s *Provider) Subscribe(topic []byte, qos byte, sub Subscriber, profile interface{}) (byte, error) {
	if !message.ValidQos(qos) {
		return message.QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("Subscriber cannot be nil")
	}

	s.smut.Lock()
	defer s.smut.Unlock()

	if qos > MaxQosAllowed {
		qos = MaxQosAllowed
	}

	err := s.sessions.Insert(topic, qos, sub)

	return qos, err
}

func (s *Provider) Unsubscribe(topic []byte, sub Subscriber, profile interface{}) error {
	s.smut.Lock()
	defer s.smut.Unlock()
	return s.sessions.Remove(topic, sub)
}

func (s *Provider) Subscribers(topic []byte, qos byte, subs *[]Subscriber, qoss *[]byte, profile interface{}) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	s.smut.RLock()
	defer s.smut.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	return s.sessions.Match(topic, qos, subs, qoss)
}

func (s *Provider) Retain(msg *message.PublishMessage, profile interface{}) error {
	s.rmut.Lock()
	defer s.rmut.Unlock()

	// So apparently, at least according to the MQTT Conformance/Interoperability
	// Testing, that a payload of 0 means delete the retain message.
	// https://eclipse.org/paho/clients/testing/
	if len(msg.Payload()) == 0 {
		return s.retainer.Remove(msg.Topic())
	}

	return s.retainer.Insert(msg.Topic(), msg)
}

func (s *Provider) Retained(topic []byte, msgs *[]*message.PublishMessage, profile interface{}) error {
	s.rmut.RLock()
	defer s.rmut.RUnlock()

	return s.retainer.Match(topic, msgs)
}

func (s *Provider) Close() error {
	err := s.sessions.Close()
	if err != nil {
		return err
	}

	return s.retainer.Close()
}

func NewMemProvider() TopicsProvider {
	return NewProvider(NewMemRetainer(), NewMemSession())
}

func NewPudgeProvider(path string, syncIntv time.Duration) (TopicsProvider, error) {
	s, err := NewPudgeStorage(path, syncIntv)
	if err != nil {
		return nil, err
	}

	r, err := NewStorageRetainer(s)
	if err != nil {
		return nil, err
	}

	return NewProvider(r, NewMemSession()), nil
}

func init() {
	Register("mem", NewMemProvider())
	p, _ := NewPudgeProvider(DefaultDbPath, DefaultSyncIntv)
	Register("pudge", p)
}
