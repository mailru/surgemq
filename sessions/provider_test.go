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

package sessions

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mailru/surgemq/message"
	"github.com/recoilme/pudge"
	"github.com/stretchr/testify/require"
)

func TestMemProvider(t *testing.T) {
	st := NewMemProvider()

	for i := 0; i < 10; i++ {
		st.New(fmt.Sprintf("%d", i), struct{}{})
	}

	require.Equal(t, 10, st.Count(), "Incorrect length.")

	for i := 0; i < 10; i++ {
		sess, err := st.Get(fmt.Sprintf("%d", i), struct{}{})
		require.NoError(t, err, "Unable to Get() session #%d", i)

		require.Equal(t, fmt.Sprintf("%d", i), sess.id, "Incorrect ID")
	}

	for i := 0; i < 5; i++ {
		st.Del(fmt.Sprintf("%d", i), struct{}{})
	}

	require.Equal(t, 5, st.Count(), "Incorrect length.")
}

func initSession(sess *Session) {
	sess.initted = true
	sess.mu = sync.Mutex{}
	sess.topics = make(map[string]byte)
	connMsg := message.NewConnectMessage()
	connMsg.SetVersion(0x04)
	connMsg.SetClientId([]byte(sess.id))
	sess.Update(connMsg)

	rtMsg := message.NewPublishMessage()
	rtMsg.SetTopic([]byte("foo"))
	rtMsg.SetPayload([]byte("payload"))
	sess.RetainMessage(rtMsg)
	sess.AddTopic("foo", 77)
}

func TestPudgeProvider(t *testing.T) {
	pvd, err := NewPudgeProvider("db/test_sess", time.Second, time.Minute)
	require.NoError(t, err, "Failed to init PudgeProvider")

	aaaSess, err := pvd.New("aaa", struct{}{})
	require.NoError(t, err, "Unable to create 'aaa' session")
	initSession(aaaSess)
	err = pvd.Save("aaa", struct{}{})
	require.NoError(t, err, "Failed to save 'aaa' session")

	bbbSess, err := pvd.New("bbb", struct{}{})
	require.NoError(t, err, "Unable to create 'bbb' session")
	initSession(bbbSess)
	err = pvd.Save("bbb", struct{}{})
	require.NoError(t, err, "Failed to save 'bbb' session")

	cccSess, err := pvd.New("ccc", struct{}{})
	require.NoError(t, err, "Unable to create 'ccc' session")
	initSession(cccSess)
	err = pvd.Save("ccc", struct{}{})
	require.NoError(t, err, "Failed to save 'ccc' session")
	require.Equal(t, 3, pvd.Count(), "Incorrect length")

	pvd.Del("bbb", struct{}{})
	require.Equal(t, 2, pvd.Count(), "Incorrect length")

	pvd.Close()

	pvd, err = NewPudgeProvider("db/test_sess", time.Second, time.Minute)
	require.NoError(t, err, "Failed to reinit PudgeProvider")
	require.Equal(t, 2, pvd.Count(), "Incorrect length")

	sess, err := pvd.Get("aaa", struct{}{})
	require.NoError(t, err, "Unable to find 'aaa' session")
	require.Equal(
		t, newPudgeSession(aaaSess), newPudgeSession(sess),
		"Session 'aaa' differs from original session",
	)

	sess, err = pvd.Get("ccc", struct{}{})
	require.NoError(t, err, "Unable to find 'ccc' session")
	require.Equal(
		t, newPudgeSession(cccSess), newPudgeSession(sess),
		"Session 'ccc' differs from original session",
	)
}

func TestPudgeProviderExpiration(t *testing.T) {
	pvd, err := NewPudgeProvider("db/test_sess", time.Second, time.Second)
	require.NoError(t, err, "Failed to init PudgeProvider")

	aaaSess, err := pvd.New("aaa", struct{}{})
	require.NoError(t, err, "Unable to create 'aaa' session")
	initSession(aaaSess)
	err = pvd.Save("aaa", struct{}{})
	require.NoError(t, err, "Failed to save 'aaa' session")

	time.Sleep(1200 * time.Millisecond)

	pvd.New("bbb", struct{}{})

	time.Sleep(100 * time.Millisecond)

	_, err = pvd.Get("aaa", struct{}{})
	if err != pudge.ErrKeyNotFound {
		t.Errorf("found key 'aaa' after expiration with error '%v'", err)
	}
}
