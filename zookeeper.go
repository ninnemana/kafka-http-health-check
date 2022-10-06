package main

import (
    "context"
    "flag"
    "fmt"
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

    zkAddrs    = flag.String("zk-addr", "127.0.0.1", "list of zookeeper instances to connect")
    serverPort = flag.String("http-port", "8080", "HTTP port to serve status on")
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
    for {
        if err := w.check(); err != nil {
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

func (w *worker) check() error {
    var status bool
    defer func(v bool) {
        w.Lock()
        w.up = v
        w.Unlock()
    }(status)

    children, _, err := w.client.Children("/brokers/ids")
    if err != nil {
        return fmt.Errorf("failed to get broker list: %w", err)
    }

    if len(children) > 0 {
        status = true
    }
    
    return nil
}
