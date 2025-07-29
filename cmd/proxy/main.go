package main

import (
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/fx"

	"github.com/temporalio/s2s-proxy/client"
	"github.com/temporalio/s2s-proxy/config"
	"github.com/temporalio/s2s-proxy/proxy"
	"github.com/temporalio/s2s-proxy/transport"
)

const (
	ProxyVersion = "0.0.1"
)

type ProxyParams struct {
	fx.In

	ConfigProvider config.ConfigProvider
	Proxy          *proxy.Proxy
	Logger         log.Logger
}

type DebugResponse struct {
	Timestamp     time.Time                  `json:"timestamp"`
	Connections   []transport.ConnectionInfo `json:"connections"`
	ActiveStreams []proxy.StreamInfo         `json:"active_streams"`
	StreamCount   int                        `json:"stream_count"`
}

func run(args []string) error {
	app := buildCLIOptions()
	return app.Run(args)
}

func buildCLIOptions() *cli.App {
	app := cli.NewApp()
	app.Name = "s2s-proxy"
	app.Usage = "Temporal proxy between servers"
	app.Version = ProxyVersion

	app.Commands = []*cli.Command{
		{
			Name:  "start",
			Usage: "Starts the proxy.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     config.ConfigPathFlag,
					Usage:    "path to proxy config yaml file",
					Required: true,
				},
				&cli.StringFlag{
					Name:     config.LogLevelFlag,
					Usage:    "Set log level(debug, info, warn, error). Default level is info",
					Required: false,
				},
			},
			Action: startProxy,
		},
	}

	return app
}

func startPProfHTTPServer(logger log.Logger, c config.ProfilingConfig, proxyInstance *proxy.Proxy) {
	addr := c.PProfHTTPAddress
	if len(addr) == 0 {
		return
	}

	// Add debug endpoint handler
	http.HandleFunc("/debug/connections", func(w http.ResponseWriter, r *http.Request) {
		handleDebugConnections(w, r, proxyInstance, logger)
	})

	go func() {
		logger.Info("Start pprof http server", tag.NewStringTag("address", addr))
		if err := http.ListenAndServe(addr, nil); err != nil {
			panic(err)
		}
	}()
}

func handleDebugConnections(w http.ResponseWriter, r *http.Request, proxyInstance *proxy.Proxy, logger log.Logger) {
	w.Header().Set("Content-Type", "application/json")

	var connections []transport.ConnectionInfo
	var activeStreams []proxy.StreamInfo
	var streamCount int

	// Get connection information from the proxy
	if proxyInstance != nil {
		connections = proxyInstance.GetConnectionInfo()
	}

	// Get active streams information
	streamTracker := proxy.GetGlobalStreamTracker()
	activeStreams = streamTracker.GetActiveStreams()
	streamCount = streamTracker.GetStreamCount()

	response := DebugResponse{
		Timestamp:     time.Now(),
		Connections:   connections,
		ActiveStreams: activeStreams,
		StreamCount:   streamCount,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error("Failed to encode debug response", tag.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func startProxy(c *cli.Context) error {
	var proxyParams ProxyParams

	var logCfg log.Config
	if logLevel := c.String(config.LogLevelFlag); len(logLevel) != 0 {
		logCfg.Level = logLevel
	}

	app := fx.New(
		fx.Provide(func() *cli.Context { return c }),
		fx.Provide(func() log.Logger {
			return log.NewZapLogger(log.BuildZapLogger(logCfg))
		}),
		config.Module,
		transport.Module,
		client.Module,
		proxy.Module,
		fx.Populate(&proxyParams),
	)

	if err := app.Err(); err != nil {
		return err
	}

	cfg := proxyParams.ConfigProvider.GetS2SProxyConfig()
	startPProfHTTPServer(proxyParams.Logger, cfg.ProfilingConfig, proxyParams.Proxy)

	if err := proxyParams.Proxy.Start(); err != nil {
		return err
	}

	// Waits until interrupt signal from OS arrives
	<-interruptCh()

	proxyParams.Proxy.Stop()
	return nil
}

func main() {
	if err := run(os.Args); err != nil {
		panic(err)
	}
}

// InterruptCh returns channel which will get data when system receives interrupt signal.
func interruptCh() <-chan interface{} {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	ret := make(chan interface{}, 1)
	go func() {
		s := <-c
		ret <- s
		close(ret)
		signal.Stop(c)
	}()

	return ret
}
