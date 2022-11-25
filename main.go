package main

import (
	"os"

	"github.com/cosmosquad-labs/blockparser/cmd"
)

func main() {

	if err := cmd.PebbleBlockParserCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
