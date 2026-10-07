package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var testTime = time.Unix(1700000000, 0)

func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("not JSON: %s", b)
	}
	return m
}

func TestNewCompletionShape(t *testing.T) {
	c := NewCompletion("chatcmpl-1", "qwen-7b", testTime, "hello", "stop", Usage{PromptTokens: 3, CompletionTokens: 5, TotalTokens: 8})
	m := decode(t, c.JSON())
	if m["object"] != "chat.completion" || m["model"] != "qwen-7b" || m["id"] != "chatcmpl-1" || m["created"] != float64(1700000000) {
		t.Fatalf("unexpected envelope %v", m)
	}
	choice := m["choices"].([]any)[0].(map[string]any)
	msg := choice["message"].(map[string]any)
	if msg["role"] != "assistant" || msg["content"] != "hello" || choice["finish_reason"] != "stop" {
		t.Fatalf("unexpected choice %v", choice)
	}
	if _, has := choice["delta"]; has {
		t.Fatal("a non-streaming response must not carry a delta")
	}
	usage := m["usage"].(map[string]any)
	if usage["prompt_tokens"] != float64(3) || usage["completion_tokens"] != float64(5) || usage["total_tokens"] != float64(8) {
		t.Fatalf("unexpected usage %v", usage)
	}
}

func TestNewChunkShapes(t *testing.T) {
	role := decode(t, NewChunk("id", "m", testTime, Delta{Role: "assistant", Content: Str("")}, "").JSON())
	if role["object"] != "chat.completion.chunk" {
		t.Fatalf("unexpected object %v", role["object"])
	}
	c0 := role["choices"].([]any)[0].(map[string]any)
	d0 := c0["delta"].(map[string]any)
	if d0["role"] != "assistant" || d0["content"] != "" {
		t.Fatalf("the role chunk must carry an empty content string, got %v", d0)
	}
	if v, has := c0["finish_reason"]; !has || v != nil {
		t.Fatalf("finish_reason must be present and null before the end, got %v (present %v)", v, has)
	}
	if _, has := role["usage"]; has {
		t.Fatal("chunks must not carry usage")
	}

	tok := decode(t, NewChunk("id", "m", testTime, Delta{Content: Str("tok0 ")}, "").JSON())
	d := tok["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if d["content"] != "tok0 " {
		t.Fatalf("unexpected delta %v", d)
	}
	if _, has := d["role"]; has {
		t.Fatal("content chunks must not repeat the role")
	}

	last := NewChunk("id", "m", testTime, Delta{}, "length").JSON()
	if !strings.Contains(string(last), `"delta":{}`) || !strings.Contains(string(last), `"finish_reason":"length"`) {
		t.Fatalf("unexpected final chunk %s", last)
	}
}
