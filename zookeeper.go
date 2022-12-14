package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"
)

var (
	CheckInterval      = time.Second * 1
	DefaultHTTPTimeout = time.Second * 5

	zkAddrs      = flag.String("zk-addr", "127.0.0.1", "list of zookeeper instances to connect")
	serverPort   = flag.String("http-port", "8080", "HTTP port to serve status on")
	additionalIP = flag.String("local-ip", "", "IP address to match on")
)

func main() {
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	c, _, err := zk.Connect(strings.Split(*zkAddrs, ","), time.Second)
	if err != nil {
		logger.Error("failed to connect to zookeeper", zap.Error(err))

		return
	}

	wrk := worker{
		client: c,
		up:     false,
		log:    logger,
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := wrk.work(ctx); err != nil {
			logger.Error("fell out of background worker", zap.Error(err))
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if !wrk.up {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte("down")); err != nil {
				logger.Error("failed to write response to HTTP body", zap.Error(err))
			}

			return
		}

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("up")); err != nil {
			logger.Error("failed to write response to HTTP body", zap.Error(err))
		}
	})

	server := http.Server{
		Addr:              ":" + *serverPort,
		Handler:           nil,
		TLSConfig:         nil,
		ReadTimeout:       DefaultHTTPTimeout,
		ReadHeaderTimeout: DefaultHTTPTimeout,
		WriteTimeout:      DefaultHTTPTimeout,
		IdleTimeout:       DefaultHTTPTimeout,
	}

	if err := server.ListenAndServe(); err != nil {
		logger.Error("fell out of HTTP server", zap.Error(err))
	}
}

type worker struct {
	client *zk.Conn
	log    *zap.Logger
	up     bool

	sync.Mutex
}

func (w *worker) work(ctx context.Context) error {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for {
		if err := w.check(
			conn.LocalAddr().(*net.UDPAddr).IP.String(),
			*additionalIP,
		); err != nil {
			w.log.Error("zookeeper query failed", zap.Error(err))
			continue
		}

		// if the context has been cancelled, we'll return
		// otherwise, we'll pause for our interval
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("context was cancelled: %w", err)
			}

			return nil
		case <-time.After(CheckInterval):
		}
	}
}

var noBrokersErr = errors.New("no brokers found")

func (w *worker) check(addresses ...string) error {
	var status bool
	defer func() {
		w.log.Info("setting status", zap.Bool("up", status))

		w.Lock()
		w.up = status
		w.Unlock()
	}()

	children, _, err := w.client.Children("/brokers/ids")
	if err != nil {
		return fmt.Errorf("failed to get broker list: %w", err)
	}

	w.log.Info("fetched brokers", zap.Int("count", len(children)))
	if len(children) == 0 {
		return noBrokersErr
	}

	for _, child := range children {
		w.log.Info("fetching broker", zap.String("broker", child))

		data, _, err := w.client.Get("/brokers/ids/" + child)
		if err != nil {
			w.log.Error("failed to fetch broker details: %w", zap.Error(err))

			return fmt.Errorf("failed to fetch broker details: %w", err)
		}

		var b broker
		if err := json.Unmarshal(data, &b); err != nil {
			w.log.Error("failed to parse broker details: %w", zap.Error(err))

			return fmt.Errorf("failed to parse broker details: %w", err)
		}

		w.log.Info("fetching broker", zap.Strings("endpoints", b.Endpoints))

		for _, e := range b.Endpoints {
			for _, ip := range addresses {
				w.log.Info("comparing with IP", zap.String("endpoint", e), zap.String("local", ip))

				if strings.Contains(e, ip) {
					w.log.Info("broker matches IP", zap.String("endpoint", e), zap.String("local", ip))
					status = true
					return nil
				}
			}
		}
	}

	return nil
}

type broker struct {
	Endpoints []string `json:"endpoints"`
}
