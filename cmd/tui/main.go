package main

import (
	"fmt"
	"os"

	"burstkv/internal/tui"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: tui <controller-address>   e.g. tui http://localhost:8080")
		os.Exit(1)
	}
	if err := tui.Run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
