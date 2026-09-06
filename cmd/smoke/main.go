// smoke verifies infrastructure only; it is not an order API or inventory worker.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"kafka-recovery-lab/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: smoke kafka|mysql|mysql-write ID|mysql-read ID|mysql-clean ID")
	}
	timeout, err := config.Timeout()
	if err != nil {
		return err
	}
	base, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(base, timeout)
	defer cancel()
	command := os.Args[1]
	switch command {
	case "kafka":
		return kafkaSmoke(ctx)
	case "mysql", "mysql-write", "mysql-read", "mysql-clean":
		id := ""
		if command != "mysql" {
			if len(os.Args) != 3 || len(os.Args[2]) == 0 || len(os.Args[2]) > 64 {
				return fmt.Errorf("%s requires a probe ID of 1..64 bytes", command)
			}
			id = os.Args[2]
		}
		return mysqlSmoke(ctx, command, id)
	default:
		return fmt.Errorf("unknown smoke command")
	}
}
