package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/rijuyuezhu/pkudisk-sync/internal/apppaths"
	"github.com/rijuyuezhu/pkudisk-sync/internal/cli"
)

func main() {
	paths, err := apppaths.Default()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pkudisk-sync: %v\n", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), shutdownSignals()...)
	defer stop()
	if err := cli.New(paths, os.Stdout, os.Stderr).Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "pkudisk-sync: %v\n", err)
		os.Exit(1)
	}
}
