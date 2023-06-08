package metrics

import (
	"context"
	"net"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

var log = logging.Logger("provider/metrics")

type Server struct {
	exporter   *otelprom.Exporter
	httpserver http.Server
	listen     net.Listener
}

// NewServer instantiates a new server that upon start exposes the collected metrics as Prometheus
// metrics.
func NewServer(listenAddr string) (*Server, error) {
	listen, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	// Create Prometheus Exporter and register its Collector.
	exporter, err := otelprom.New()
	if err != nil {
		return nil, err
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	return &Server{
		exporter: exporter,
		httpserver: http.Server{
			Handler: mux,
		},
		listen: listen,
	}, nil
}

func (s *Server) Start() error {
	log.Infow("Starting metrics server", "listenAddr", s.listen.Addr())
	go func() {
		err := s.httpserver.Serve(s.listen)
		log.Infow("Stopped metric server", "err", err)
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	sErr := s.httpserver.Shutdown(ctx)
	eErr := s.exporter.Shutdown(ctx)
	if sErr != nil {
		return sErr
	}
	return eErr
}
