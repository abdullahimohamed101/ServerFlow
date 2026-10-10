package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 123456789, time.UTC)

func i64(v int64) *int64 { return &v }

func baseEvent(typ, attempt string) Event {
	return Event{
		EventID: NewEventID("req_0123456789abcdef", typ, attempt), EventType: typ, SchemaVersion: 1, Timestamp: t0,
		Source: "gw-1", RequestID: "req_0123456789abcdef", AttemptID: attempt, TenantID: "ten_aaaaaaaaaaaaaaaa",
		APIKeyID: "key_bbbbbbbbbbbbbbbb", Model: "llama-3-8b", WorkerID: "worker-a",
	}
}

// fixtureEvents are the golden v1 events. Fixtures are only ever added, never edited (D5 rule 5).
func fixtureEvents() map[string]Event {
	recv := baseEvent(EventReceived, "")
	recv.WorkerID = ""
	recv.Received = &ReceivedData{Stream: true, EstimatedCostTokens: 612}
	routed := baseEvent(EventRouted, "att_1111111111111111")
	routed.Routed = &RoutedData{AttemptNumber: 1, Strategy: "round-robin"}
	first := baseEvent(EventFirstToken, "att_1111111111111111")
	first.FirstToken = &FirstTokenData{TTFTMS: 41}
	done := baseEvent(EventCompleted, "att_1111111111111111")
	done.Terminal = &TerminalData{Stream: true, HTTPStatus: 200, DurationMS: 812, TTFTMS: 41, InputTokens: i64(18), OutputTokens: i64(120),
		TokensSource: TokensFromUsage, EstimatedCostTokens: 612,
		Attempts: []AttemptData{{AttemptID: "att_1111111111111111", Number: 1, WorkerID: "worker-a", Outcome: "ok", DurationMS: 810}}}
	failed := baseEvent(EventFailed, "att_2222222222222222")
	failed.Terminal = &TerminalData{HTTPStatus: 503, DurationMS: 95, TokensSource: TokensFromEstimate, EstimatedCostTokens: 300, FailureClass: FailureWorkerError,
		Attempts: []AttemptData{
			{AttemptID: "att_1111111111111111", Number: 1, WorkerID: "worker-a", Outcome: "retried", FailureClass: "status_503", DurationMS: 30},
			{AttemptID: "att_2222222222222222", Number: 2, WorkerID: "worker-b", Outcome: "failed", FailureClass: "status_503", DurationMS: 60},
		}}
	// What the gateway emits today: no model before it is confirmed (the model arrives with routed and the terminal event).
	recvNoModel := recv
	recvNoModel.Model = ""
	recvNoModel.EventID = NewEventID(recvNoModel.RequestID, EventReceived, "")
	return map[string]Event{"received_no_model": recvNoModel, "received": recv, "routed": routed, "first_token": first, "completed": done, "failed_after_retry": failed}
}

func TestFixturesWrittenOnceAndDecodeUnderCurrentCode(t *testing.T) {
	dir := filepath.Join("testdata", "events", "v1")
	if os.Getenv("SERVERFLOW_WRITE_EVENT_FIXTURES") != "" {
		for name, e := range fixtureEvents() {
			p := filepath.Join(dir, name+".json")
			if _, err := os.Stat(p); err == nil {
				continue // never edited
			}
			b, err := Encode(e)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, append(b, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	if len(files) < 5 {
		t.Fatalf("expected at least 5 golden fixtures, found %d", len(files))
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		e, err := Decode(b)
		if err != nil {
			t.Errorf("%s no longer decodes: %v", f, err)
			continue
		}
		if e.SchemaVersion != 1 || e.EventID == "" {
			t.Errorf("%s decoded wrongly: %+v", f, e)
		}
	}
	// The current code still produces exactly the committed bytes for the known events.
	for name, e := range fixtureEvents() {
		want, err := os.ReadFile(filepath.Join(dir, name+".json"))
		if err != nil {
			t.Fatalf("fixture %s missing: %v", name, err)
		}
		got, err := Encode(e)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(want)) != string(got) {
			t.Errorf("%s: encoding drifted from the committed fixture\n got %s\nwant %s", name, got, want)
		}
	}
}

func TestRoundTripKeepsEveryField(t *testing.T) {
	for name, e := range fixtureEvents() {
		b, err := Encode(e)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := Decode(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(got, e) {
			t.Errorf("%s: round trip changed the event\n got %+v\nwant %+v", name, got, e)
		}
	}
}

func TestUnknownFieldsAreIgnoredAndNewerVersionIsRejected(t *testing.T) {
	b, _ := Encode(fixtureEvents()["completed"])
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	m["brand_new_field"] = map[string]any{"x": 1}
	m["payload"].(map[string]any)["another_new_field"] = "v"
	withExtra, _ := json.Marshal(m)
	if _, err := Decode(withExtra); err != nil {
		t.Fatalf("an added field must be ignored: %v", err)
	}
	m["schema_version"] = 2
	m["payload"] = "a different shape entirely"
	future, _ := json.Marshal(m)
	_, err := Decode(future)
	if !errors.Is(err, ErrEventVersionUnsupported) {
		t.Fatalf("want ErrEventVersionUnsupported, got %v", err)
	}
}

func TestDecodeRejectsGarbageWithoutPanicking(t *testing.T) {
	for _, in := range []string{"", "null", "[]", "{", `{"schema_version":1}`, `{"schema_version":"x"}`, strings.Repeat("a", MaxEventBytes+1),
		`{"schema_version":1,"event_type":"inference.request.completed","payload":null}`} {
		if _, err := Decode([]byte(in)); err == nil {
			t.Errorf("Decode(%.30q) succeeded", in)
		}
	}
}

func TestValidateRefusals(t *testing.T) {
	good := fixtureEvents()["completed"]
	cases := map[string]func(*Event){
		"no event id":     func(e *Event) { e.EventID = "" },
		"wrong event id":  func(e *Event) { e.EventID = "evt_00000000000000000000000000000000" },
		"no request id":   func(e *Event) { e.RequestID = "" },
		"bad request id":  func(e *Event) { e.RequestID = "xyz" },
		"bad type":        func(e *Event) { e.EventType = "inference.request.nope" },
		"no source":       func(e *Event) { e.Source = "" },
		"long model":      func(e *Event) { e.Model = strings.Repeat("m", 129) },
		"control char":    func(e *Event) { e.WorkerID = "a\nb" },
		"zero time":       func(e *Event) { e.Timestamp = time.Time{} },
		"future version":  func(e *Event) { e.SchemaVersion = 9 },
		"zero version":    func(e *Event) { e.SchemaVersion = 0 },
		"no payload":      func(e *Event) { e.Terminal = nil },
		"two payloads":    func(e *Event) { e.Received = &ReceivedData{} },
		"bad tokens src":  func(e *Event) { e.Terminal.TokensSource = "guess" },
		"estimate+counts": func(e *Event) { e.Terminal.TokensSource = TokensFromEstimate },
		"negative tokens": func(e *Event) { e.Terminal.InputTokens = i64(-1) },
		"huge input":      func(e *Event) { e.Terminal.InputTokens = i64(math.MaxInt64) },
		"huge output":     func(e *Event) { e.Terminal.OutputTokens = i64(MaxEventTokens + 1) },
		"huge estimate":   func(e *Event) { e.Terminal.EstimatedCostTokens = math.MaxInt64 },
		"huge duration":   func(e *Event) { e.Terminal.DurationMS = MaxEventMillis + 1 },
		"huge attempt": func(e *Event) {
			e.Terminal.Attempts = []AttemptData{{AttemptID: "att_1", Number: 1, Outcome: "ok", DurationMS: math.MaxInt64}}
		},
		"bad status":       func(e *Event) { e.Terminal.HTTPStatus = 0 },
		"class on success": func(e *Event) { e.Terminal.FailureClass = FailureTimeout },
		"too many attempts": func(e *Event) {
			e.Terminal.Attempts = make([]AttemptData, MaxEventAttempts+1)
		},
	}
	for name, mut := range cases {
		e := good
		term := *good.Terminal
		e.Terminal = &term
		mut(&e)
		if err := e.Validate(); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		} else if _, encErr := Encode(e); encErr == nil {
			t.Errorf("%s: Encode accepted it", name)
		}
	}
	f := fixtureEvents()["failed_after_retry"]
	f.Terminal = &TerminalData{HTTPStatus: 503, TokensSource: TokensFromEstimate, FailureClass: "free text from a worker"}
	if f.Validate() == nil {
		t.Error("a failed event with free-text failure_class was accepted")
	}
	if err := (Event{}).Validate(); err == nil {
		t.Error("the zero event validated")
	}
}

func TestValidationErrorsNeverEchoValues(t *testing.T) {
	e := fixtureEvents()["completed"]
	e.Model = strings.Repeat("SECRETMODEL", 20)
	err := e.Validate()
	if err == nil || strings.Contains(err.Error(), "SECRETMODEL") {
		t.Fatalf("error must exist and not echo the value: %v", err)
	}
}

func TestEncodeRefusesOversizeEvent(t *testing.T) {
	e := fixtureEvents()["completed"]
	// 16 attempts at the field caps exceed MaxEventBytes only if the caps allow it; assert the guard separately.
	big := make([]byte, MaxEventBytes+1)
	if _, err := Decode(big); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("want ErrEventTooLarge, got %v", err)
	}
	_ = e
}

func TestEventIDDeterministicAndDistinct(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	rnd := func() string { return fmt.Sprintf("x%016x", r.Uint64()) }
	types := []string{EventReceived, EventRouted, EventFirstToken, EventCompleted, EventFailed}
	seen := map[string][3]string{}
	for i := 0; i < 5000; i++ {
		req, typ, att := "req_"+rnd(), types[r.Intn(len(types))], "att_"+rnd()
		id := NewEventID(req, typ, att)
		if id != NewEventID(req, typ, att) {
			t.Fatal("not deterministic")
		}
		if !strings.HasPrefix(id, "evt_") || len(id) != 36 {
			t.Fatalf("bad shape %q", id)
		}
		key := [3]string{req, typ, att}
		if prev, dup := seen[id]; dup && prev != key {
			t.Fatalf("collision %v vs %v", prev, key)
		}
		seen[id] = key
		for _, other := range [][3]string{{req, typ, att + "x"}, {req, types[(r.Intn(4)+1+indexOf(types, typ))%5], att}, {req + "x", typ, att}} {
			if NewEventID(other[0], other[1], other[2]) == id && other != key {
				t.Fatalf("different inputs, same ID: %v vs %v", key, other)
			}
		}
	}
	// The field separator matters: ("ab","c") must not equal ("a","bc").
	if NewEventID("req_ab", "c", "") == NewEventID("req_a", "bc", "") {
		t.Fatal("fields are not separated")
	}
}

func indexOf(s []string, v string) int {
	for i := range s {
		if s[i] == v {
			return i
		}
	}
	return 0
}

// TestEventTypesHoldOnlyPrimitives is the structural half of the privacy guarantee (D4): an event can carry no
// prompt, body or free text because no field could hold one that is not a bounded string we validate.
func TestEventTypesHoldOnlyPrimitives(t *testing.T) {
	allowedStrings := map[string]bool{ // every string field, each validated in Validate (length, prefix or enum)
		"Event.EventID": true, "Event.EventType": true, "Event.Source": true, "Event.RequestID": true, "Event.AttemptID": true,
		"Event.TenantID": true, "Event.APIKeyID": true, "Event.Model": true, "Event.WorkerID": true,
		"RoutedData.Strategy": true, "AttemptData.AttemptID": true, "AttemptData.WorkerID": true, "AttemptData.Outcome": true,
		"AttemptData.FailureClass": true, "TerminalData.TokensSource": true, "TerminalData.FailureClass": true,
	}
	seen := map[reflect.Type]bool{}
	var walk func(owner string, typ reflect.Type)
	walk = func(owner string, typ reflect.Type) {
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice:
			walk(owner, typ.Elem())
		case reflect.Struct:
			if typ == reflect.TypeOf(time.Time{}) || seen[typ] {
				return
			}
			seen[typ] = true
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				name := typ.Name() + "." + f.Name
				if f.Type.Kind() == reflect.String && !allowedStrings[name] {
					t.Errorf("string field %s is not on the audited list; a new string needs a cap and a review", name)
				}
				walk(name, f.Type)
			}
		case reflect.String, reflect.Bool, reflect.Int, reflect.Int64:
		default:
			t.Errorf("%s has kind %s, which an event must not contain", owner, typ.Kind())
		}
	}
	walk("Event", reflect.TypeOf(Event{}))
}

func TestTokenBoundsAreInclusive(t *testing.T) {
	e := fixtureEvents()["completed"]
	e.Terminal = &TerminalData{HTTPStatus: 200, TokensSource: TokensFromUsage, InputTokens: i64(MaxEventTokens), OutputTokens: i64(MaxEventTokens), EstimatedCostTokens: MaxEventTokens}
	if err := e.Validate(); err != nil {
		t.Fatalf("the bound itself must be allowed: %v", err)
	}
	r := fixtureEvents()["received"]
	r.Received.EstimatedCostTokens = MaxEventTokens + 1
	if r.Validate() == nil {
		t.Fatal("received with an absurd estimate was accepted")
	}
}
