# BlockParser 

## Support
- PebbleDB 

## Install, build

```bash
# install
go mod edit -replace github.com/tendermint/tm-db=github.com/baabeetaa/tm-db@pebble
go mod tidy
go install -tags pebbledb -ldflags "-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=pebbledb" ./...
```

## How to
```bash
# Usage : blockparser [chain-dir] [start-height] [end-height]
blockparser ~/.evmosd 402001 432001

output : blockparser
```
Loaded :  /Users/guest/.evmosd/data/
Input Start Height : 402001
Input End Height : 432001
Latest Height : 475830
410000
420000
430000
Done! check the output files on current dir : data-402001-432001.csv
```