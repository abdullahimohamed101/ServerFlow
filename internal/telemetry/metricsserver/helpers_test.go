package metricsserver

import (
	"net/http"
	"net/http/httptest"
)

func newTestServer(h http.Handler) *httptest.Server { return httptest.NewServer(h) }
