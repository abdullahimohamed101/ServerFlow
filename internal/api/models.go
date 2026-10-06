package api

import "encoding/json"

type modelList struct {
	Object string        `json:"object"`
	Data   []modelObject `json:"data"`
}

type modelObject struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ModelsBody returns the OpenAI-format JSON body for GET /v1/models.
func ModelsBody(models []string) []byte {
	list := modelList{Object: "list", Data: make([]modelObject, 0, len(models))}
	for _, m := range models {
		list.Data = append(list.Data, modelObject{ID: m, Object: "model", OwnedBy: "serverflow"})
	}
	b, _ := json.Marshal(list)
	return b
}
