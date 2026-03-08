package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	amqpbackend "github.com/openjobspec/ojs-backend-amqp/internal/amqp"
	"github.com/openjobspec/ojs-backend-amqp/internal/core"
	"github.com/openjobspec/ojs-backend-amqp/internal/events"
	"github.com/openjobspec/ojs-backend-amqp/internal/scheduler"
	"github.com/openjobspec/ojs-backend-amqp/internal/server"
)

func main() {
	cfg := server.LoadConfig()
	if cfg.APIKey == "" && !cfg.AllowInsecureNoAuth {
		slog.Error("refusing to start without API authentication",
			"hint", "set OJS_API_KEY or OJS_ALLOW_INSECURE_NO_AUTH=true for local development")
		os.Exit(1)
	}
	if cfg.AllowInsecureNoAuth {
		slog.Warn("⚠️  RUNNING WITHOUT AUTHENTICATION — set OJS_API_KEY for production")
	}

	// Create AMQP backend
	backend, err := amqpbackend.New(cfg.AMQPURL)
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
	router := server.NewRouter(backend, cfg, broker, broker)
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

	// Print startup banner
	printBanner(cfg)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

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
