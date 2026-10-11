// Command healthcheck is the container health probe: `healthcheck http://127.0.0.1:8080/healthz` exits 0 only if
// the URL answers with a 2xx status within three seconds. It exists because the runtime images (distroless)
// contain no shell, curl or wget. It uses only the standard library, accepts loopback hosts only (it checks the
// container it runs in, never the network), does not follow redirects, never sends credentials and never prints
// the response body.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

const timeout = 3 * time.Second

func main() {
	os.Exit(run(os.Args[1:], timeout, os.Stderr))
}

func run(args []string, timeout time.Duration, stderr io.Writer) int {
	if len(args) != 1 {
		_, _ = fmt.Fprintln(stderr, "usage: healthcheck URL (http://127.0.0.1:PORT/PATH)")
		return 2
	}
	u, err := url.Parse(args[0])
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		_, _ = fmt.Fprintln(stderr, "healthcheck: the argument must be an http:// or https:// URL without credentials")
		return 2
	}
	if !loopback(u.Hostname()) {
		_, _ = fmt.Fprintln(stderr, "healthcheck: the host must be a loopback address")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "healthcheck: bad request")
		return 2
	}
	client := &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport:     &http.Transport{DisableKeepAlives: true, Proxy: nil},
	}
	resp, err := client.Do(req)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "healthcheck: request failed")
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = fmt.Fprintf(stderr, "healthcheck: status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
