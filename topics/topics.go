// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package topics deals with MQTT topic names, topic filters and subscriptions.
// - "Topic name" is a / separated string that could contain #, * and $
// - / in topic name separates the string into "topic levels"
// - # is a multi-level wildcard, and it must be the last character in the
//   topic name. It represents the parent and all children levels.
// - + is a single level wildwcard. It must be the only character in the
//   topic level. It represents all names in the current level.
// - $ is a special character that says the topic is a system level topic
package topics

import (
	"fmt"

	"github.com/surgemq/message"
)

const (
	// MWC is the multi-level wildcard
	MWC = "#"

	// SWC is the single level wildcard
	SWC = "+"

	// SEP is the topic level separator
	SEP = "/"

	// SYS is the starting character of the system level topics
	SYS = "$"

	// Both wildcards
	_WC = "#+"
)

var (
	providers = make(map[string]TopicsProvider)
)

type Subscriber interface {
	OnPublish(msg *message.PublishMessage) error
}

// TopicsProvider
type TopicsProvider interface {
	Subscribe(topic []byte, qos byte, subscriber Subscriber, profile interface{}) (byte, error)
	Unsubscribe(topic []byte, subscriber Subscriber, profile interface{}) error
	Subscribers(topic []byte, qos byte, subs *[]Subscriber, qoss *[]byte, profile interface{}) error
	Retain(msg *message.PublishMessage, profile interface{}) error
	Retained(topic []byte, msgs *[]*message.PublishMessage, profile interface{}) error
	Close() error
}

func Register(name string, provider TopicsProvider) {
	if provider == nil {
		panic("topics: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		panic("topics: Register called twice for provider " + name)
	}

	providers[name] = provider
}

func Unregister(name string) {
	delete(providers, name)
}

type Manager struct {
	p TopicsProvider
}

func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("session: unknown provider %q", providerName)
	}

	return &Manager{p: p}, nil
}

func (this *Manager) Subscribe(topic []byte, qos byte, subscriber Subscriber, profile interface{}) (byte, error) {
	return this.p.Subscribe(topic, qos, subscriber, profile)
}

func (this *Manager) Unsubscribe(topic []byte, subscriber Subscriber, profile interface{}) error {
	return this.p.Unsubscribe(topic, subscriber, profile)
}

func (this *Manager) Subscribers(topic []byte, qos byte, subs *[]Subscriber, qoss *[]byte, profile interface{}) error {
	return this.p.Subscribers(topic, qos, subs, qoss, profile)
}

func (this *Manager) Retain(msg *message.PublishMessage, profile interface{}) error {
	return this.p.Retain(msg, profile)
}

func (this *Manager) Retained(topic []byte, msgs *[]*message.PublishMessage, profile interface{}) error {
	return this.p.Retained(topic, msgs, profile)
}

func (this *Manager) Close() error {
	return this.p.Close()
}
