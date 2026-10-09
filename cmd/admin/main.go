// Command admin (serverflow-admin) manages ServerFlow's PostgreSQL metadata: migrations,
// tenants, API keys and model configs. The database comes from SERVERFLOW_POSTGRES_DSN or the
// config file; the DSN is never accepted as a flag, printed or echoed. A new API key is printed
// once, on standard output, and can never be shown again.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"serverflow/internal/auth"
	"serverflow/internal/config"
	"serverflow/internal/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

const usage = `usage: serverflow-admin [--config FILE] <command>

  migrate up | status
  tenant  create NAME [--rpm N] [--tpm N] [--max-concurrent N] [--priority 0-2] [--models a,b]
          list | show TENANT | suspend TENANT | activate TENANT
          set-quota TENANT [--rpm N] [--tpm N] [--max-concurrent N] [--priority 0-2]
          set-models TENANT (--all | --none | --models a,b)
  key     create --tenant TENANT [--label TEXT] [--expires 30d]
          list [--tenant TENANT] | revoke KEY_ID_OR_PREFIX
  model   add NAME [--display-name TEXT] [--max-tokens N] [--notes TEXT]
          list | enable NAME | disable NAME

TENANT is a tenant name or ID. The database comes from SERVERFLOW_POSTGRES_DSN or the config file.
`

type app struct {
	out, err io.Writer
	st       *postgres.Store
}

func run(ctx context.Context, args []string, out, errw io.Writer) int {
	cfgPath := ""
	if len(args) >= 2 && args[0] == "--config" {
		cfgPath, args = args[1], args[2:]
	}
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		_, _ = io.WriteString(errw, usage)
		return 2
	}
	a := &app{out: out, err: errw}
	if err := a.dispatch(ctx, cfgPath, args); err != nil {
		var ue usageError
		if errors.As(err, &ue) {
			_, _ = fmt.Fprintf(errw, "serverflow-admin: %s\n\n%s", ue, usage)
			return 2
		}
		_, _ = fmt.Fprintf(errw, "serverflow-admin: %s\n", describe(err))
		return 1
	}
	return 0
}

type usageError string

func (u usageError) Error() string { return string(u) }

func usagef(format string, a ...any) error { return usageError(fmt.Sprintf(format, a...)) }

// describe turns store errors into plain messages.
func describe(err error) string {
	switch {
	case errors.Is(err, postgres.ErrNotFound):
		return "not found"
	case errors.Is(err, postgres.ErrConflict):
		return "already exists"
	case errors.Is(err, postgres.ErrInvalid):
		return strings.TrimPrefix(err.Error(), postgres.ErrInvalid.Error()+": ")
	}
	return err.Error()
}

func (a *app) dispatch(ctx context.Context, cfgPath string, args []string) error {
	group, rest := args[0], args[1:]
	switch group {
	case "migrate", "tenant", "key", "model":
	default:
		return usagef("unknown command %q", truncate(group))
	}
	if len(rest) == 0 {
		return usagef("%s needs a subcommand", group)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	if err := postgres.CheckTransport(cfg.Postgres.DSN, cfg.Postgres.AllowInsecureTransport); err != nil {
		return err
	}
	st, err := postgres.Open(ctx, postgres.Config{DSN: cfg.Postgres.DSN, MaxConns: cfg.Postgres.MaxConns, ConnectTimeout: cfg.Postgres.ConnectTimeout})
	if err != nil {
		return err
	}
	defer st.Close()
	a.st = st
	sub, rest := rest[0], rest[1:]
	switch group {
	case "migrate":
		return a.migrate(ctx, sub, rest)
	case "tenant":
		return a.tenant(ctx, sub, rest)
	case "key":
		return a.key(ctx, sub, rest)
	default:
		return a.model(ctx, sub, rest)
	}
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// parse parses flags that may follow positional arguments, returning the positionals.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, usagef("%v", err)
		}
		args = fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}
	return pos, nil
}

func (a *app) migrate(ctx context.Context, sub string, args []string) error {
	if len(args) != 0 {
		return usagef("migrate %s takes no arguments", sub)
	}
	switch sub {
	case "up":
		done, err := a.st.MigrateUp(ctx)
		for _, m := range done {
			_, _ = fmt.Fprintf(a.out, "applied %04d_%s\n", m.Version, m.Name)
		}
		if err != nil {
			return err
		}
		if len(done) == 0 {
			_, _ = fmt.Fprintln(a.out, "database is up to date")
		}
		return nil
	case "status":
		st, err := a.st.MigrationStatuses(ctx)
		for _, m := range st {
			state := "pending"
			if m.Applied {
				state = "applied " + m.AppliedAt.UTC().Format(time.RFC3339)
			}
			_, _ = fmt.Fprintf(a.out, "%04d_%s\t%s\n", m.Version, m.Name, state)
		}
		return err
	}
	return usagef("unknown migrate subcommand %q", truncate(sub))
}

func (a *app) tenant(ctx context.Context, sub string, args []string) error {
	switch sub {
	case "create":
		fs := newFlags("tenant create")
		rpm, tpm, conc := fs.Int("rpm", 0, ""), fs.Int("tpm", 0, ""), fs.Int("max-concurrent", 0, "")
		prio, models := fs.Int("priority", 1, ""), fs.String("models", "", "")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		if len(pos) != 1 {
			return usagef("tenant create needs exactly one NAME")
		}
		in := postgres.TenantInput{Name: pos[0], RequestsPerMinute: *rpm, TokensPerMinute: *tpm, MaxConcurrentRequests: *conc, Priority: *prio}
		if set(fs, "models") {
			if in.AllowedModels = splitList(*models); in.AllowedModels == nil {
				in.AllowedModels = []string{}
			}
		}
		t, err := a.st.CreateTenant(ctx, in)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.out, "created tenant %s (%s)\n", t.Name, t.ID)
		return nil
	case "list":
		if len(args) != 0 {
			return usagef("tenant list takes no arguments")
		}
		ts, err := a.st.ListTenants(ctx)
		if err != nil {
			return err
		}
		for _, t := range ts {
			_, _ = fmt.Fprintf(a.out, "%s\t%s\t%s\tpriority=%d\tmodels=%s\n", t.ID, t.Name, t.Status, t.Priority, modelsText(t.AllowedModels))
		}
		return nil
	case "show":
		ref, err := one(args, "tenant show")
		if err != nil {
			return err
		}
		t, err := a.st.GetTenant(ctx, ref)
		if err != nil {
			return err
		}
		a.printTenant(t)
		ks, err := a.st.ListKeys(ctx, t.ID)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.out, "keys:         %d\n", len(ks))
		return nil
	case "suspend", "activate":
		ref, err := one(args, "tenant "+sub)
		if err != nil {
			return err
		}
		status := auth.TenantSuspended
		if sub == "activate" {
			status = auth.TenantActive
		}
		t, prev, err := a.st.SetTenantStatus(ctx, ref, status)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.out, "tenant %s: %s -> %s\n", t.Name, prev, t.Status)
		if status == auth.TenantSuspended {
			_, _ = fmt.Fprintln(a.out, "note: gateways notice within auth.cache_ttl (default 30s)")
		}
		return nil
	case "set-quota":
		fs := newFlags("tenant set-quota")
		rpm, tpm, conc, prio := fs.Int("rpm", 0, ""), fs.Int("tpm", 0, ""), fs.Int("max-concurrent", 0, ""), fs.Int("priority", 0, "")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		if len(pos) != 1 {
			return usagef("tenant set-quota needs exactly one TENANT")
		}
		var q postgres.Quota
		if set(fs, "rpm") {
			q.RequestsPerMinute = rpm
		}
		if set(fs, "tpm") {
			q.TokensPerMinute = tpm
		}
		if set(fs, "max-concurrent") {
			q.MaxConcurrentRequests = conc
		}
		if set(fs, "priority") {
			q.Priority = prio
		}
		if q == (postgres.Quota{}) {
			return usagef("tenant set-quota needs at least one of --rpm, --tpm, --max-concurrent, --priority")
		}
		t, err := a.st.SetTenantQuota(ctx, pos[0], q)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.out, "tenant %s: rpm=%d tpm=%d max_concurrent=%d priority=%d (stored; not enforced until a later phase)\n",
			t.Name, t.RequestsPerMinute, t.TokensPerMinute, t.MaxConcurrentRequests, t.Priority)
		return nil
	case "set-models":
		fs := newFlags("tenant set-models")
		all, none, models := fs.Bool("all", false, ""), fs.Bool("none", false, ""), fs.String("models", "", "")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		if len(pos) != 1 {
			return usagef("tenant set-models needs exactly one TENANT")
		}
		n := 0
		for _, b := range []bool{*all, *none, set(fs, "models")} {
			if b {
				n++
			}
		}
		if n != 1 {
			return usagef("tenant set-models needs exactly one of --all, --none, --models a,b")
		}
		var list []string // nil: all models
		switch {
		case *none:
			list = []string{}
		case set(fs, "models"):
			if list = splitList(*models); len(list) == 0 {
				return usagef("--models needs at least one model; use --none to allow none")
			}
		}
		t, err := a.st.SetTenantModels(ctx, pos[0], list)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.out, "tenant %s: models=%s\n", t.Name, modelsText(t.AllowedModels))
		return nil
	}
	return usagef("unknown tenant subcommand %q", truncate(sub))
}

func (a *app) printTenant(t postgres.Tenant) {
	_, _ = fmt.Fprintf(a.out, "id:           %s\nname:         %s\nstatus:       %s\npriority:     %d\nrpm:          %d\ntpm:          %d\nmax_concurrent: %d\nmodels:       %s\ncreated:      %s\n",
		t.ID, t.Name, t.Status, t.Priority, t.RequestsPerMinute, t.TokensPerMinute, t.MaxConcurrentRequests, modelsText(t.AllowedModels), t.CreatedAt.UTC().Format(time.RFC3339))
}

func (a *app) key(ctx context.Context, sub string, args []string) error {
	switch sub {
	case "create":
		fs := newFlags("key create")
		tenant, label, expires := fs.String("tenant", "", ""), fs.String("label", "", ""), fs.String("expires", "", "")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		if len(pos) != 0 || *tenant == "" {
			return usagef("key create needs --tenant TENANT (and no positional arguments)")
		}
		var exp *time.Time
		if *expires != "" {
			d, err := parseExpiry(*expires)
			if err != nil {
				return err
			}
			t := time.Now().Add(d).UTC()
			exp = &t
		}
		var k postgres.APIKey
		var plaintext string
		for try := 0; ; try++ {
			var prefix string
			var hash []byte
			plaintext, prefix, hash = auth.GenerateKey()
			k, err = a.st.CreateKey(ctx, *tenant, *label, exp, prefix, hash)
			if errors.Is(err, postgres.ErrConflict) && try < 5 {
				continue // a prefix collision: draw another key
			}
			break
		}
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.err, "Created key %s for tenant %s. This is the only time the key is shown; store it now.\n", k.ID, k.TenantName)
		if k.ExpiresAt != nil {
			_, _ = fmt.Fprintf(a.err, "It expires %s.\n", k.ExpiresAt.UTC().Format(time.RFC3339))
		}
		_, _ = fmt.Fprintln(a.out, plaintext)
		return nil
	case "list":
		fs := newFlags("key list")
		tenant := fs.String("tenant", "", "")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		if len(pos) != 0 {
			return usagef("key list takes only --tenant")
		}
		ks, err := a.st.ListKeys(ctx, *tenant)
		if err != nil {
			return err
		}
		for _, k := range ks {
			_, _ = fmt.Fprintf(a.out, "%s\t%s\tprefix=%s\t%s\texpires=%s\tlast_used=%s\tlabel=%q\n", k.ID, k.TenantName, k.Prefix, k.Status, tsText(k.ExpiresAt), tsText(k.LastUsedAt), k.Label)
		}
		return nil
	case "revoke":
		ref, err := one(args, "key revoke")
		if err != nil {
			return err
		}
		if prefix, _, perr := auth.ParseKey(ref); perr == nil {
			// A whole key was pasted. Only its public prefix identifies the key; the secret half must
			// not travel any further, and it is now in this shell's history.
			_, _ = fmt.Fprintln(a.err, "warning: you passed a complete API key. Only its prefix is needed and is being used.")
			_, _ = fmt.Fprintln(a.err, "         The secret is now in your shell history and process list: remove it from history, and rotate this key if it is still in use anywhere.")
			ref = prefix
		}
		k, was, err := a.st.RevokeKey(ctx, ref)
		if err != nil {
			return err
		}
		if was {
			_, _ = fmt.Fprintf(a.out, "revoked key %s (tenant %s)\nnote: gateways stop accepting it within auth.cache_ttl (default 30s)\n", k.ID, k.TenantName)
		} else {
			_, _ = fmt.Fprintf(a.out, "key %s (tenant %s) was already revoked\n", k.ID, k.TenantName)
		}
		return nil
	}
	return usagef("unknown key subcommand %q", truncate(sub))
}

func (a *app) model(ctx context.Context, sub string, args []string) error {
	switch sub {
	case "add":
		fs := newFlags("model add")
		display, notes, maxTok := fs.String("display-name", "", ""), fs.String("notes", "", ""), fs.Int("max-tokens", 0, "")
		pos, err := parse(fs, args)
		if err != nil {
			return err
		}
		if len(pos) != 1 {
			return usagef("model add needs exactly one NAME")
		}
		m := postgres.Model{Name: pos[0], DisplayName: *display, Notes: *notes}
		if set(fs, "max-tokens") {
			m.MaxTokensLimit = maxTok
		}
		if m, err = a.st.AddModel(ctx, m); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.out, "added model %s (%s)\n", m.Name, m.Status)
		return nil
	case "list":
		if len(args) != 0 {
			return usagef("model list takes no arguments")
		}
		ms, err := a.st.ListModels(ctx)
		if err != nil {
			return err
		}
		for _, m := range ms {
			lim := "-"
			if m.MaxTokensLimit != nil {
				lim = strconv.Itoa(*m.MaxTokensLimit)
			}
			_, _ = fmt.Fprintf(a.out, "%s\t%s\tmax_tokens=%s\tdisplay=%q\n", m.Name, m.Status, lim, m.DisplayName)
		}
		return nil
	case "enable", "disable":
		name, err := one(args, "model "+sub)
		if err != nil {
			return err
		}
		status := postgres.ModelEnabled
		if sub == "disable" {
			status = postgres.ModelDisabled
		}
		m, prev, err := a.st.SetModelStatus(ctx, name, status)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.out, "model %s: %s -> %s\n", m.Name, prev, m.Status)
		return nil
	}
	return usagef("unknown model subcommand %q", truncate(sub))
}

func one(args []string, what string) (string, error) {
	if len(args) != 1 {
		return "", usagef("%s needs exactly one argument", what)
	}
	return args[0], nil
}

func set(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func modelsText(m []string) string {
	if m == nil {
		return "all"
	}
	if len(m) == 0 {
		return "none"
	}
	return strings.Join(m, ",")
}

func tsText(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

// parseExpiry accepts "30d", "12h", "90m" or any Go duration; it must be positive and at most 10 years.
func parseExpiry(s string) (time.Duration, error) {
	var d time.Duration
	if n, ok := strings.CutSuffix(s, "d"); ok {
		days, err := strconv.Atoi(n)
		if err != nil || days <= 0 || days > 3650 {
			return 0, usagef("--expires %q: days must be a whole number from 1 to 3650", truncate(s))
		}
		d = time.Duration(days) * 24 * time.Hour
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, usagef("--expires %q is not a duration such as 30d, 12h or 90m", truncate(s))
		}
	}
	if d <= 0 || d > 3650*24*time.Hour {
		return 0, usagef("--expires must be positive and at most 10 years")
	}
	return d, nil
}
