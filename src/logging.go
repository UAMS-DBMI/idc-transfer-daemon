package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
)

// newLogger returns an slog.Logger writing to stderr, and additionally to a
// GELF UDP target when gelfAddr is non-empty. The second return value reports
// whether GELF output is actually wired up — false when gelfAddr is empty or
// the UDP dial failed. A dial failure logs a warning to stderr and falls back
// to stderr-only; the daemon does not fail to start on a misconfigured log
// endpoint.
func newLogger(gelfAddr, gelfTag string) (*slog.Logger, bool) {
	stderr := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	if gelfAddr == "" {
		return slog.New(stderr), false
	}
	conn, err := net.Dial("udp", gelfAddr)
	if err != nil {
		slog.New(stderr).Warn("gelf: dial failed; logging to stderr only",
			"addr", gelfAddr, "err", err)
		return slog.New(stderr), false
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	gelf := &gelfHandler{sink: &gelfSink{
		conn:   conn,
		host:   host,
		tag:    gelfTag,
		minLvl: slog.LevelInfo,
	}}
	return slog.New(&fanoutHandler{handlers: []slog.Handler{stderr, gelf}}), true
}

// redactDSN returns a copy of dsn with the password masked. Handles both the
// URI form (postgres://user:pass@host/db) and libpq keyword=value form. Used
// to keep credentials out of the startup config log, which fans out to GELF.
func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" && u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), "***")
			return u.String()
		}
		return dsn
	}
	return dsnPasswordKV.ReplaceAllString(dsn, "password=***")
}

var dsnPasswordKV = regexp.MustCompile(`(?i)password\s*=\s*\S+`)

// warnMisspelledEnv looks for environment variables whose name contains
// `token` but isn't `expected` or any of `siblings`, and logs each as a
// possible typo. Catches the case where an optional config (e.g. GELF_ADDR)
// silently no-ops because the operator wrote `GELF_ADRR` or similar. `token`
// should be the distinctive shared prefix of the env-var family (e.g. "GELF");
// `siblings` lists other known names in that family so a correctly-set
// GELF_TAG doesn't get flagged when GELF_ADDR is the one missing.
func warnMisspelledEnv(log *slog.Logger, expected, token string, siblings ...string) {
	upperToken := strings.ToUpper(token)
	known := map[string]bool{expected: true}
	for _, s := range siblings {
		known[s] = true
	}
	for _, kv := range os.Environ() {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		name := kv[:eq]
		if known[name] {
			continue
		}
		if strings.Contains(strings.ToUpper(name), upperToken) {
			log.Warn("possible env var typo — expected name is unset but a similarly named one is set",
				"expected", expected, "found", name)
		}
	}
}

// fanoutHandler dispatches each record to every wrapped handler. WithAttrs and
// WithGroup propagate to all children so context attached via logger.With(...)
// reaches both sinks identically.
type fanoutHandler struct {
	handlers []slog.Handler
}

func (f *fanoutHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, lvl) {
			return true
		}
	}
	return false
}

func (f *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, h := range f.handlers {
		if !h.Enabled(ctx, r.Level) {
			continue
		}
		if err := h.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (f *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		out[i] = h.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: out}
}

func (f *fanoutHandler) WithGroup(name string) slog.Handler {
	out := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		out[i] = h.WithGroup(name)
	}
	return &fanoutHandler{handlers: out}
}

// gelfMaxDatagram caps each UDP write well under the typical 8KB safe MTU for
// UDP. Records that exceed this get short_message truncated rather than chunked
// — the daemon's log lines are small structured records, not free-form bodies.
const gelfMaxDatagram = 8192

// DefaultGelfTag is emitted as the `_tag` additional field on every GELF record
// when GELF_TAG is unset, so Graylog streams/pipelines can route this daemon's
// logs by tag out of the box.
const DefaultGelfTag = "idc-transfer-daemon"

// gelfSink owns the resources shared across every handler derived via
// WithAttrs/WithGroup: the UDP socket, the write mutex serialising datagrams,
// the host tag, and the minimum level. Holding it by pointer keeps `go vet`
// happy and ensures all derived handlers share one lock over one conn.
type gelfSink struct {
	conn   net.Conn
	host   string
	tag    string
	minLvl slog.Level
	mu     sync.Mutex
}

// gelfHandler encodes records as GELF 1.1 JSON and writes one datagram per
// record to the shared sink.
type gelfHandler struct {
	sink   *gelfSink
	attrs  []slog.Attr
	groups []string
}

func (g *gelfHandler) Enabled(_ context.Context, lvl slog.Level) bool {
	return lvl >= g.sink.minLvl
}

func (g *gelfHandler) Handle(_ context.Context, r slog.Record) error {
	payload := map[string]any{
		"version":       "1.1",
		"host":          g.sink.host,
		"short_message": r.Message,
		"timestamp":     float64(r.Time.UnixNano()) / 1e9,
		"level":         syslogLevel(r.Level),
		"_tag":          g.sink.tag,
	}
	prefix := ""
	if len(g.groups) > 0 {
		prefix = strings.Join(g.groups, ".") + "."
	}
	for _, a := range g.attrs {
		addGelfAttr(payload, prefix, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		addGelfAttr(payload, prefix, a)
		return true
	})

	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(buf) > gelfMaxDatagram {
		over := len(buf) - gelfMaxDatagram + 32
		msg := r.Message
		if over < len(msg) {
			payload["short_message"] = msg[:len(msg)-over] + "...[truncated]"
		} else {
			payload["short_message"] = "[message too large for gelf datagram]"
		}
		buf, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	g.sink.mu.Lock()
	defer g.sink.mu.Unlock()
	_, err = g.sink.conn.Write(buf)
	return err
}

func (g *gelfHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &gelfHandler{
		sink:   g.sink,
		attrs:  append(append([]slog.Attr(nil), g.attrs...), attrs...),
		groups: g.groups,
	}
}

func (g *gelfHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return g
	}
	return &gelfHandler{
		sink:   g.sink,
		attrs:  g.attrs,
		groups: append(append([]string(nil), g.groups...), name),
	}
}

func addGelfAttr(payload map[string]any, prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Key == "" {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, sub := range a.Value.Group() {
			addGelfAttr(payload, prefix+a.Key+".", sub)
		}
		return
	}
	payload["_"+sanitizeGelfKey(prefix+a.Key)] = a.Value.Any()
}

// GELF additional-field keys must match ^[\w\.\-]*$, and `_id` is reserved by
// Graylog. Map foreign runes to underscore and rename a bare `id` so users can
// pass natural attr names without surprise drops.
func sanitizeGelfKey(k string) string {
	var b strings.Builder
	b.Grow(len(k))
	for _, r := range k {
		switch {
		case r == '_' || r == '.' || r == '-',
			r >= '0' && r <= '9',
			r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "id" {
		return "id_"
	}
	return out
}

// syslogLevel maps slog levels to GELF's syslog severity (0=emerg .. 7=debug).
func syslogLevel(l slog.Level) int {
	switch {
	case l >= slog.LevelError:
		return 3
	case l >= slog.LevelWarn:
		return 4
	case l >= slog.LevelInfo:
		return 6
	default:
		return 7
	}
}

var (
	_ slog.Handler = (*gelfHandler)(nil)
	_ slog.Handler = (*fanoutHandler)(nil)
)
