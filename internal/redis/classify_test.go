package redis

import (
	"context"
	"errors"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
	"io"
	"net"
	"syscall"
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
		"unexpected eof":  {fmt.Errorf("read: %w", io.ErrUnexpectedEOF), errConnection},
		"client closed":   {goredis.ErrClosed, errConnection},
		"refused errno":   {fmt.Errorf("dial: %w", syscall.ECONNREFUSED), errConnection},
		"reset errno":     {fmt.Errorf("read: %w", syscall.ECONNRESET), errConnection},
		"broken pipe":     {fmt.Errorf("write: %w", syscall.EPIPE), errConnection},
		"server reply":    {serverReply("NOSCRIPT No matching script"), errScript},
		"anything else":   {errors.New("surprise"), errOther},
	} {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("%s: %s, want %s", name, errorKindNames[got], errorKindNames[tc.want])
		}
	}
}
