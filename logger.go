package echoratelimiter

type Logger interface {
	Debug(msg string, args ...any)
}

type nilLogger struct{}

func (n *nilLogger) Debug(_ string, _ ...any) {}
