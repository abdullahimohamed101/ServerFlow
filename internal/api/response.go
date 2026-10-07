package api

import (
	"encoding/json"
	"time"
)

// Wire types for OpenAI-compatible chat completion responses. The gateway
// relays upstream bytes untouched; these types are for components that
// produce responses, such as the mock worker.

// Usage reports token counts for a completed request.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Message is a complete assistant message in a non-streaming response.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Delta is an incremental update in a streaming chunk. A nil Content is
// omitted (as in the final chunk); a non-nil empty Content is sent as "".
type Delta struct {
	Role    string  `json:"role,omitempty"`
	Content *string `json:"content,omitempty"`
}

// Choice is one completion alternative. A non-streaming response sets
// Message; a streaming chunk sets Delta. FinishReason is null until the end.
type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message,omitempty"`
	Delta        *Delta   `json:"delta,omitempty"`
	FinishReason *string  `json:"finish_reason"`
}

// ChatCompletion is a chat.completion response or a chat.completion.chunk.
type ChatCompletion struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// Str returns a pointer to s, for optional string fields.
func Str(s string) *string { return &s }

// NewCompletion builds a non-streaming response.
func NewCompletion(id, model string, created time.Time, content, finishReason string, usage Usage) ChatCompletion {
	return ChatCompletion{
		ID: id, Object: "chat.completion", Created: created.Unix(), Model: model,
		Choices: []Choice{{Message: &Message{Role: "assistant", Content: content}, FinishReason: Str(finishReason)}},
		Usage:   &usage,
	}
}

// NewChunk builds one streaming chunk. finishReason is empty for all but the
// last chunk.
func NewChunk(id, model string, created time.Time, delta Delta, finishReason string) ChatCompletion {
	c := Choice{Delta: &delta}
	if finishReason != "" {
		c.FinishReason = Str(finishReason)
	}
	return ChatCompletion{
		ID: id, Object: "chat.completion.chunk", Created: created.Unix(), Model: model,
		Choices: []Choice{c},
	}
}

// JSON returns the compact JSON encoding of c.
func (c ChatCompletion) JSON() []byte {
	b, _ := json.Marshal(c)
	return b
}
