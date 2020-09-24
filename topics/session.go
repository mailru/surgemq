package topics

type MemSession struct {
	sroot *snode
}

func NewMemSession() *MemSession {
	return &MemSession{
		sroot: newSNode(),
	}
}

func (s *MemSession) Insert(topic []byte, qos byte, sub Subscriber) error {
	return s.sroot.sinsert(topic, qos, sub)
}

func (s *MemSession) Remove(topic []byte, sub Subscriber) error {
	return s.sroot.sremove(topic, sub)
}

func (s *MemSession) Match(topic []byte, qos byte, subs *[]Subscriber, qoss *[]byte) error {
	return s.sroot.smatch(topic, qos, subs, qoss)
}

func (s *MemSession) Close() error {
	s.sroot = nil
	return nil
}
