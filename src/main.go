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
	gcsKeyJSON := os.Getenv("GCS_KEY_JSON")
	gelfAddr := os.Getenv("GELF_ADDR")
	rawGelfTag := os.Getenv("GELF_TAG")
	gelfTag := rawGelfTag
	if gelfTag == "" {
		gelfTag = DefaultGelfTag
	}

	logger, gelfActive, closeLogger := newLogger(gelfAddr, gelfTag)
	defer closeLogger()
	slog.SetDefault(logger)

	if os.Getenv("TRANSFER_DAEMON_DSN") != "" {
		logger.Warn("TRANSFER_DAEMON_DSN is no longer read; configure Postgres with " +
			"PGHOST, PGPORT, PGUSER, PGPASSWORD and PGDATABASE instead")
	}
	if gcsKeyJSON == "" {
		warnMisspelledEnv(logger, "GCS_KEY_JSON", "GCS_KEY")
	}
	if gelfAddr == "" {
		warnMisspelledEnv(logger, "GELF_ADDR", "GELF", "GELF_TAG")
	}
	if rawGelfTag == "" {
		warnMisspelledEnv(logger, "GELF_TAG", "GELF", "GELF_ADDR")
	}

	logger.Info("daemon starting",
		"gcs_key_json_set", gcsKeyJSON != "",
		"gcs_service_account", serviceAccountEmail(gcsKeyJSON),
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
