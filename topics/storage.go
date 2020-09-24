package topics

import (
	"math"
	"time"

	"github.com/mailru/surgemq/message"
	"github.com/recoilme/pudge"
)

const (
	DefaultSyncIntv = time.Second
	DefaultDbPath   = "db/message"
)

type PudgeStorage struct {
	db *pudge.Db
}

func NewPudgeStorage(path string, syncIntv time.Duration) (*PudgeStorage, error) {
	db, err := pudge.Open(path, &pudge.Config{
		FileMode:     pudge.DefaultConfig.FileMode,
		DirMode:      pudge.DefaultConfig.DirMode,
		SyncInterval: int(math.Round(syncIntv.Seconds())),
		StoreMode:    pudge.DefaultConfig.StoreMode,
	})

	if err != nil {
		return nil, err
	}

	return &PudgeStorage{db: db}, nil
}

func (d *PudgeStorage) messages() ([]*message.PublishMessage, error) {
	dbKeys, err := d.db.Keys(nil, 0, 0, true)
	if err != nil {
		return nil, err
	}

	messages := []*message.PublishMessage{}
	for _, key := range dbKeys {
		var buf []byte
		err := d.db.Get(key, &buf)
		if err != nil {
			return nil, err
		}

		msg := message.NewPublishMessage()
		msg.Decode(buf)
		messages = append(messages, msg)
	}

	return messages, nil
}

func (d *PudgeStorage) insert(topic []byte, msg *message.PublishMessage) error {
	buf := make([]byte, msg.Len())
	msg.Encode(buf)
	return d.db.Set(topic, buf)
}

func (d *PudgeStorage) remove(topic []byte) error {
	return d.db.Delete(topic)
}

func (d *PudgeStorage) close() error {
	return d.db.Close()
}

func (d *PudgeStorage) clear() error {
	return d.db.DeleteFile()
}
