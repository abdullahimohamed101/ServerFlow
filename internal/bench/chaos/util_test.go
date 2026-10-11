package chaos

import (
	"errors"
	"os"
)

var errReload = errors.New("the reloaded result lost its chaos section or resilience value")

func mkdir(dir string) error { return os.MkdirAll(dir, 0o755) }
