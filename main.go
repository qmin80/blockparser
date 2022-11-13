package main

import (
	"os"
	"strings"
	"github.com/cosmosquad-labs/blockparser/cmd"
)

func main() {

	if strings.HasPrefix(os.Args[1], "rpc") {
		if err := cmd.RPCParserCmd().Execute(); err != nil {
			os.Exit(1)
		}
	} else if strings.HasPrefix(os.Args[1], "consensus") {
		if err := cmd.ConsensusParserCmd().Execute(); err != nil {
			os.Exit(1)
		}
	} else {
		if err := cmd.BlockParserCmd().Execute(); err != nil {
			os.Exit(1)
		}
	}
}
