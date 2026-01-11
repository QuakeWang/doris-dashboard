package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/QuakeWang/doris-dashboard/apps/agentd/internal/api"
)

func main() {
	var listenAddr string
	var exportTimeout time.Duration
	flag.StringVar(&listenAddr, "listen", "127.0.0.1:12306", "HTTP listen address")
	flag.DurationVar(&exportTimeout, "export-timeout", 60*time.Second, "Doris audit log export timeout")
	flag.Parse()
	if exportTimeout <= 0 {
		exportTimeout = 60 * time.Second
	}
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		log.Printf("invalid --listen %q: %v", listenAddr, err)
		os.Exit(2)
	}
	ip := net.ParseIP(host)
	if ip == nil && strings.EqualFold(host, "localhost") {
		ip = net.IPv4(127, 0, 0, 1)
	}
	if ip == nil || !ip.IsLoopback() {
		log.Printf("--listen must bind to loopback only (127.0.0.1, [::1], or localhost), got %q", listenAddr)
		os.Exit(2)
	}

	handler := api.NewServer(nil, exportTimeout)
	httpServer := &http.Server{
		Addr:              listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      exportTimeout + 10*time.Second,
		IdleTimeout:       30 * time.Second,
	}

	log.Printf("agentd listening on http://%s", listenAddr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("server error: %v", err)
		os.Exit(1)
	}
}
