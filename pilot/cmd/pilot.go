package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"github.com/spf13/cobra"

	pilot "github.com/regret-io/regret/pilot-go"
)

var rootCmd = &cobra.Command{
	Use:   "regret-pilot",
	Short: "Regret chaos engineering pilot server",
	RunE:  run,
}

func init() {
	rootCmd.Flags().String("data-dir", "/data", "Base data directory")
	rootCmd.Flags().Uint16("http-port", 8080, "HTTP server port")
	rootCmd.Flags().String("namespace", "regret", "Kubernetes namespace")
	rootCmd.Flags().String("auth-password", "", "Basic auth password for write operations (empty = disabled)")
}

func run(cmd *cobra.Command, _ []string) error {
	zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	slog.SetDefault(slog.New(slogzerolog.Option{Logger: &zerologLogger}.NewZerologHandler()))

	dataDir, _ := cmd.Flags().GetString("data-dir")
	httpPort, _ := cmd.Flags().GetUint16("http-port")
	namespace, _ := cmd.Flags().GetString("namespace")
	authPw, _ := cmd.Flags().GetString("auth-password")

	var authPassword *string
	if authPw != "" {
		authPassword = &authPw
	}

	slog.Info("starting regret-pilot", "data_dir", dataDir, "http_port", httpPort, "namespace", namespace)

	p, err := pilot.New(dataDir, httpPort, namespace, authPassword)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return p.Start(ctx)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
