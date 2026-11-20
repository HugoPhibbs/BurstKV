package main

import (
	"os"

	"burstkv/internal/cli"
)

func main() {
	if err := cli.Start(); err != nil {
		os.Exit(1) // cobra has already printed the error
	}
}
