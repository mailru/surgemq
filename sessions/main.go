package sessions

func init() {
	Register("mem", NewMemProvider())

	p, err := NewPudgeProvider(DefaultDbPath, DefaultSyncIntv, DefaultTTL)
	if err != nil {
		panic("cannot register pudge provider")
	}

	Register("pudge", p)
}
