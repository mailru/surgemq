// Copyright (c) 2018 The SurgeMQ Authors. All rights reserved.
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

package service

import (
	"github.com/surge/glog"
	"github.com/surgemq/message"

	"github.com/RepentantGopher/surgemq/topics"
)

type Subscriber interface {
	topics.Subscriber
	OnComplete(msg, ack message.Message, err error) error
}

type subscriber struct {
	service *service
}

func(s *subscriber) OnPublish(msg *message.PublishMessage) error {
	if err := s.service.publish(msg, nil); err != nil {
		glog.Errorf("service/onPublish: Error publishing message: %v", err)
		return err
	}

	return nil
}

func(s *subscriber) OnComplete(msg, ack message.Message, err error) error {
	return nil
}

func newSubscriber(service *service) *subscriber {
	return &subscriber{
		service: service,
	}
}
