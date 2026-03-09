package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	amqpbackend "github.com/openjobspec/ojs-backend-amqp/internal/amqp"
	"github.com/openjobspec/ojs-backend-amqp/internal/core"
	"github.com/openjobspec/ojs-backend-amqp/internal/events"
	ojsgrpc "github.com/openjobspec/ojs-backend-amqp/internal/grpc"
	"github.com/openjobspec/ojs-backend-amqp/internal/scheduler"
	"github.com/openjobspec/ojs-backend-amqp/internal/server"
	"github.com/openjobspec/ojs-backend-amqp/internal/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg := server.LoadConfig()
	if err := cfg.BaseConfig.Validate(); err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	// Initialize OpenTelemetry tracing (no-op if not configured)
	otelShutdown, err := tracing.Setup("ojs-backend-amqp")
	if err != nil {
		slog.Warn("failed to initialize tracing, continuing without it", "error", err)
	} else {
		defer otelShutdown()
	}

	// Create AMQP backend
	var backendOpts []amqpbackend.Option
	if cfg.PersistPath != "" {
		backendOpts = append(backendOpts, amqpbackend.WithPersist(cfg.PersistPath))
		slog.Info("persistence enabled", "path", cfg.PersistPath)
	}
	backend, err := amqpbackend.New(cfg.AMQPURL, backendOpts...)
	if err != nil {
		slog.Error("failed to initialize AMQP backend", "error", err)
		os.Exit(1)
	}
	defer backend.Close()

	// Start background scheduler (cron firing, retryable promotion)
	sched := scheduler.New(backend)
	sched.Start()
	defer sched.Stop()

	// Initialize event broker for real-time SSE support
	broker := events.NewBroker()
	defer broker.Close()

	// Create HTTP server
	router := server.NewRouter(backend, cfg, broker, broker, server.WithEventLister(broker))
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// Start HTTP server
	go func() {
		slog.Info("OJS HTTP server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Start gRPC server
	grpcServer := grpc.NewServer()
	ojsgrpc.Register(grpcServer, backend, ojsgrpc.WithEventSubscriber(broker))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("ojs.v1.OJSService", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(grpcServer)

	go func() {
		lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
		if err != nil {
			slog.Error("failed to listen for gRPC", "port", cfg.GRPCPort, "error", err)
			os.Exit(1)
		}
		slog.Info("OJS gRPC server listening", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server error", "error", err)
			os.Exit(1)
		}
	}()

	// Print startup banner
	printBanner(cfg)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	grpcServer.GracefulStop()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	slog.Info("server stopped")
}

func printBanner(cfg server.Config) {
	banner := `
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║      ██████╗      ██╗███████╗       █████╗ ███╗   ███╗ ██████╗ ██████╗
║     ██╔═══██╗     ██║██╔════╝      ██╔══██╗████╗ ████║██╔═══██╗██╔══██╗
║     ██║   ██║     ██║███████╗█████╗███████║██╔████╔██║██║   ██║██████╔╝
║     ██║   ██║██   ██║╚════██║╚════╝██╔══██║██║╚██╔╝██║██║▄▄██║██╔═══╝
║     ╚██████╔╝╚█████╔╝███████║      ██║  ██║██║ ╚═╝ ██║╚██████╔╝██║
║      ╚═════╝  ╚════╝ ╚══════╝      ╚═╝  ╚═╝╚═╝     ╚═╝ ╚═══╝  ╚═╝
║                                                           ║
║                Open Job Spec - AMQP Backend              ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
`
	fmt.Print(banner)
	fmt.Printf("  Version:            %s\n", core.OJSVersion)
	fmt.Printf("  Backend:            AMQP (RabbitMQ)\n")
	fmt.Printf("  AMQP URL:           %s\n", cfg.AMQPURL)
	fmt.Println()
	fmt.Printf("  HTTP Server:        http://localhost:%s\n", cfg.Port)
	fmt.Printf("  gRPC Server:        localhost:%s\n", cfg.GRPCPort)
	fmt.Println()

	if cfg.APIKey != "" {
		fmt.Println("  🔒 Authentication:  ENABLED (API key required)")
	} else {
		fmt.Println("  ⚠️  Authentication:  DISABLED (development mode)")
	}
	fmt.Println()
	fmt.Println("  Press Ctrl+C to stop")
	fmt.Println()
}
