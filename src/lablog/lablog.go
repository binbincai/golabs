package lablog

import (
	"fmt"
	"go.uber.org/zap"
	"sync"
)

type replica struct {
	gid int
	rid int
}

type Logger struct {
	mu sync.Mutex
	open bool
	prefix string
	logs map[replica]*zap.Logger
}

func New(open bool, prefix string) *Logger {
	l := &Logger{}
	l.open = open
	l.prefix = prefix
	l.logs = make(map[replica]*zap.Logger)
	return l
}

func (l *Logger) Printf(gid, rid int, format string, a ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.open {
		return
	}
	r := replica{
		gid: gid,
		rid: rid,
	}
	if _, ok := l.logs[r]; !ok {
		logFileName := fmt.Sprintf("%s_gid_%d_rid_%d.log", l.prefix, r.gid, r.rid)
		config := zap.NewDevelopmentConfig()
		config.OutputPaths = []string{logFileName}
		l.logs[r], _ = config.Build()
	}
	l.logs[r].Info(fmt.Sprintf(format, a...))
}

func Assert(cond bool) {
	if !cond {
		panic("")
	}
}

func Assert2(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}