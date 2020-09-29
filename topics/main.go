package topics

func init() {
	Register("mem", NewMemProvider())
	p, err := NewPudgeProvider(DefaultDbPath, DefaultSyncIntv)
	if err != nil {
		panic("cannot register pudge provider")
	}
	Register("pudge", p)
}
