package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

type serverReply string

func (e serverReply) Error() string { return string(e) }
func (e serverReply) RedisError()   {}

func TestClassifyPutsEveryFailureInAFixedKind(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want int
	}{
		"deadline":        {context.DeadlineExceeded, errTimeout},
		"wrapped timeout": {fmt.Errorf("x: %w", context.DeadlineExceeded), errTimeout},
		"refused":         {&net.OpError{Op: "dial", Err: errors.New("connection refused")}, errConnection},
		"eof":             {io.EOF, errConnection},
		"closed":          {net.ErrClosed, errConnection},
		"server reply":    {serverReply("NOSCRIPT No matching script"), errScript},
		"anything else":   {errors.New("surprise"), errOther},
	} {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("%s: %s, want %s", name, errorKindNames[got], errorKindNames[tc.want])
		}
	}
}
