package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	dsn := os.Getenv("TRANSFER_DAEMON_DSN")
	gcsKeyFile := os.Getenv("GCS_KEY_FILE")
	gelfAddr := os.Getenv("GELF_ADDR")
	rawGelfTag := os.Getenv("GELF_TAG")
	gelfTag := rawGelfTag
	if gelfTag == "" {
		gelfTag = DefaultGelfTag
	}

	logger, gelfActive, closeLogger := newLogger(gelfAddr, gelfTag)
	defer closeLogger()
	slog.SetDefault(logger)

	if dsn == "" {
		warnMisspelledEnv(logger, "TRANSFER_DAEMON_DSN", "TRANSFER_DAEMON")
	}
	if gcsKeyFile == "" {
		warnMisspelledEnv(logger, "GCS_KEY_FILE", "GCS_KEY")
	}
	if gelfAddr == "" {
		warnMisspelledEnv(logger, "GELF_ADDR", "GELF", "GELF_TAG")
	}
	if rawGelfTag == "" {
		warnMisspelledEnv(logger, "GELF_TAG", "GELF", "GELF_ADDR")
	}

	logger.Info("daemon starting",
		"transfer_daemon_dsn", redactDSN(dsn),
		"gcs_key_file", gcsKeyFile,
		"gelf_addr", gelfAddr,
		"gelf_tag", gelfTag,
		"gelf_active", gelfActive,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("daemon exited with error", "err", err)
		os.Exit(1)
	}
}
