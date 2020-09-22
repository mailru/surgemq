package sessions

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/mailru/surgemq/message"
	"github.com/recoilme/pudge"
)

const (
	DefaultSyncIntv = time.Second
	DefaultExpireAt = time.Hour
	DefaultDbPath   = "db/session"
)

func init() {
	p, _ := NewPudgeProvider(DefaultDbPath, DefaultSyncIntv, DefaultExpireAt)
	Register("pudge", p)
}

type pudgeSession struct {
	// cbuf is the CONNECT message buffer, this is for storing all the will stuff
	Cbuf []byte

	// rbuf is the retained PUBLISH message buffer
	Rbuf []byte

	// topics stores all the topis for this session/client
	Topics map[string]byte

	ID string
}

func (p *pudgeSession) toSession() *Session {
	sess := &Session{
		Pub1ack:  newAckqueue(defaultQueueSize),
		Pub2in:   newAckqueue(defaultQueueSize),
		Pub2out:  newAckqueue(defaultQueueSize),
		Suback:   newAckqueue(defaultQueueSize),
		Unsuback: newAckqueue(defaultQueueSize),
		Pingack:  newAckqueue(defaultQueueSize),
		Cmsg:     message.NewConnectMessage(),
		Will:     message.NewPublishMessage(),
		Retained: message.NewPublishMessage(),
		cbuf:     p.Cbuf,
		rbuf:     p.Rbuf,
		topics:   p.Topics,
		initted:  true,
		mu:       sync.Mutex{},
		id:       p.ID,
	}

	sess.Cmsg.Decode(sess.cbuf)
	sess.Retained.Decode(sess.rbuf)
	if sess.Cmsg.WillFlag() {
		sess.Will = message.NewPublishMessage()
		sess.Will.SetQoS(sess.Cmsg.WillQos())
		sess.Will.SetTopic(sess.Cmsg.WillTopic())
		sess.Will.SetPayload(sess.Cmsg.WillMessage())
		sess.Will.SetRetain(sess.Cmsg.WillRetain())
	}

	return sess
}

func newPudgeSession(sess *Session) pudgeSession {
	return pudgeSession{
		Cbuf:   sess.cbuf,
		Rbuf:   sess.rbuf,
		Topics: sess.topics,
		ID:     sess.id,
	}
}

type pudgeEntry struct {
	Sess      pudgeSession
	Timestamp time.Time
}

type pudgeConfig struct {
	path    string
	syncSec time.Duration
}

type pudgeProvider struct {
	db        *pudge.Db
	config    pudgeConfig
	expiresAt time.Duration
	smut      *sync.Mutex
	sessions  map[string]*Session
	tmut      *sync.Mutex
	lastCheck time.Time
}

func newPudgeDB(path string, syncIntv time.Duration) (*pudge.Db, error) {
	return pudge.Open(path, &pudge.Config{
		FileMode:     pudge.DefaultConfig.FileMode,
		DirMode:      pudge.DefaultConfig.DirMode,
		SyncInterval: int(math.Round(syncIntv.Seconds())),
		StoreMode:    pudge.DefaultConfig.StoreMode,
	})
}

func NewPudgeProvider(path string, syncIntv, expiresAt time.Duration) (*pudgeProvider, error) {
	db, err := newPudgeDB(path, syncIntv)
	if err != nil {
		return nil, err
	}

	return &pudgeProvider{
		db:        db,
		config:    pudgeConfig{path: path, syncSec: syncIntv},
		expiresAt: expiresAt,
		smut:      &sync.Mutex{},
		sessions:  make(map[string]*Session),
		tmut:      &sync.Mutex{},
		lastCheck: time.Now(),
	}, nil
}

func (p *pudgeProvider) expireSessions() error {
	dbKeys, err := p.db.Keys(nil, 0, 0, true)
	if err != nil {
		return err
	}

	for _, key := range dbKeys {
		var rec pudgeEntry
		err := p.db.Get(key, &rec)
		if err != nil {
			continue
		}

		isExpired := rec.Timestamp.Add(p.expiresAt).Before(time.Now())
		if isExpired {
			p.db.Delete(key)
			p.smut.Lock()
			delete(p.sessions, string(key))
			p.smut.Unlock()
		}
	}

	return nil
}

func (p *pudgeProvider) renewExpireMark() bool {
	p.tmut.Lock()
	defer p.tmut.Unlock()
	now := time.Now()
	isExpired := p.lastCheck.Add(p.expiresAt).Before(now)
	if isExpired {
		p.lastCheck = now
		return true
	}

	return false
}

func (p *pudgeProvider) New(id string, profile interface{}) (*Session, error) {
	if p.renewExpireMark() {
		go p.expireSessions()
	}

	p.smut.Lock()
	defer p.smut.Unlock()
	sess := &Session{id: id}
	p.sessions[id] = sess
	return sess, nil
}

func (p *pudgeProvider) Get(id string, profile interface{}) (*Session, error) {
	var entry pudgeEntry
	err := p.db.Get(id, &entry)
	if err != nil {
		return nil, err
	}

	err = p.db.Set(id, pudgeEntry{
		Sess:      entry.Sess,
		Timestamp: time.Now(),
	})
	if err != nil {
		return nil, err
	}

	p.smut.Lock()
	defer p.smut.Unlock()
	p.sessions[id] = entry.Sess.toSession()
	return p.sessions[id], nil
}

func (p *pudgeProvider) Del(id string, profile interface{}) {
	p.smut.Lock()
	defer p.smut.Unlock()
	delete(p.sessions, id)
	p.db.Delete(id)
}

func (p *pudgeProvider) Save(id string, profile interface{}) error {
	p.smut.Lock()
	defer p.smut.Unlock()
	sess, ok := p.sessions[id]
	if !ok {
		return fmt.Errorf("store/save not found session for id=%s", id)
	}

	return p.db.Set(id, pudgeEntry{
		Sess:      newPudgeSession(sess),
		Timestamp: time.Now(),
	})
}

func (p *pudgeProvider) Count() int {
	n, err := p.db.Count()
	if err != nil {
		return 0
	}
	return n
}

func (p *pudgeProvider) Close() error {
	p.smut.Lock()
	p.sessions = make(map[string]*Session)
	p.smut.Unlock()

	return p.db.Close()
}
