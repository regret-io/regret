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

	"github.com/regret-io/regret/pilot-go/ext"
	p "github.com/regret-io/regret/pilot-go"
)

var rootCmd = &cobra.Command{
	Use:   "regret-pilot",
	Short: "Regret chaos engineering pilot server",
	RunE:  run,
}

func init() {
	rootCmd.Flags().String("data-dir", ext.EnvOr("DATA_DIR", "/data"), "Base data directory")
}

func run(cmd *cobra.Command, _ []string) error {
	zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	slog.SetDefault(slog.New(slogzerolog.Option{Logger: &zerologLogger}.NewZerologHandler()))

	dataDir, _ := cmd.Flags().GetString("data-dir")
	httpPort := ext.EnvUint16("HTTP_PORT", 8080)
	namespace := ext.EnvOr("NAMESPACE", "regret")

	var authPassword *string
	if pw := os.Getenv("AUTH_PASSWORD"); pw != "" {
		authPassword = &pw
	}

	slog.Info("starting regret-pilot", "data_dir", dataDir, "http_port", httpPort, "namespace", namespace)

	pilot, err := p.New(dataDir, httpPort, namespace, authPassword)
	if err != nil {
		return err
	}

	// Context cancelled on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return pilot.Start(ctx)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
