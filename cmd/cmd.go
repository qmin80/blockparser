package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/store"
)

func BlockParserCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:  "blockparser [chain-dir] [start-height] [end-height]",
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			// home directory which contains chain data. e.g. ~/.crescent
			dir := args[0]

			startHeight, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}

			endHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			db, err := sdk.NewLevelDB("data/blockstore", dir)
			if err != nil {
				panic(err)
			}
			defer db.Close()

			stateDB, err := sdk.NewLevelDB("data/state", dir)
			if err != nil {
				panic(err)
			}
			defer stateDB.Close()

			blockStore := store.NewBlockStore(db)

			fmt.Println("Loaded : ", dir+"/data/")
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)
			fmt.Println("Latest Height :", blockStore.Height())

			// checking start height
			block := blockStore.LoadBlock(startHeight)
			if block == nil {
				fmt.Println(startHeight, "is not available on this data")
				for i := 0; i < 1000000000000; i++ {
					block := blockStore.LoadBlock(int64(i))
					if block != nil {
						fmt.Println("available starting Height : ", i)
						break
					}
				}
				return nil
			}

			// checking end height
			if endHeight > blockStore.Height() {
				fmt.Println(endHeight, "is not available, Latest Height : ", blockStore.Height())
				return nil
			}

			// Extract required data for LSV Performance scoring
			validatorMap := make(map[string]*ValidatorCommitInfo)
			emptyCommitMap := make(map[int]*EmptyCommit)
			proposerMap := make(map[int]*ProposerInfo)
			proposerTxMap := make(map[string]*ProposerTxInfo)

			// Gather data to create csv
			// 1) Proposer count per validator
			// 2) Proposer tx count per validator
			// 3) Commit count per validator
			for i := startHeight; i <= endHeight; i++ {

				// Extract proposer count and Txs data
				block := blockStore.LoadBlock(i)
				proposerInfo := ProposerInfo{
					Height:          i,
					ProposerAddress: fmt.Sprint(block.ProposerAddress),
					TxCount:         len(block.Txs),
				}
				proposerMap[int(i)] = &proposerInfo

				if _, ok := proposerTxMap[proposerInfo.ProposerAddress]; ok {
					proposerTxMap[proposerInfo.ProposerAddress].ProposingCount += 1
					proposerTxMap[proposerInfo.ProposerAddress].TxCount += proposerInfo.TxCount
				} else {
					proposerTxInfo := ProposerTxInfo{
						ProposerAddress: proposerInfo.ProposerAddress,
						ProposingCount:  1,
						TxCount:         proposerInfo.TxCount,
					}
					proposerTxMap[proposerInfo.ProposerAddress] = &proposerTxInfo
				}

				// Extract block commit data
				b, err := json.Marshal(blockStore.LoadBlockCommit(i))
				if err != nil {
					panic(err)
				}

				jsonString := string(b)
				var blockCommit = BlockCommit{}
				json.Unmarshal([]byte(jsonString), &blockCommit)

				// slot is the rank
				for slot, item := range blockCommit.Signatures {

					// if no signature in the slot,
					// this snippet can be removed - no use for now
					if item.ValidatorAddress == "" {

						_, ok := emptyCommitMap[slot]
						if !ok {
							emptyCommit := EmptyCommit{
								Slot: slot,
							}
							emptyCommitMap[slot] = &emptyCommit
						}

						emptyCommit := emptyCommitMap[slot]
						emptyCommit.Heights = append(emptyCommit.Heights, i)

						continue
					}

					_, ok := validatorMap[item.ValidatorAddress]
					if !ok {
						validatorCommitInfo := ValidatorCommitInfo{
							ValidatorAddress: item.ValidatorAddress,
							SlotCount:        1,
						}

						validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
							Slot:        slot,
							StartHeight: i,
							EndHeight:   i,
							CommitCount: 1,
						})

						validatorMap[item.ValidatorAddress] = &validatorCommitInfo
					} else {
						validatorCommitInfo := validatorMap[item.ValidatorAddress]
						slotCount := validatorCommitInfo.SlotCount

						if slot == validatorCommitInfo.CommitInfos[slotCount-1].Slot {
							validatorCommitInfo.CommitInfos[slotCount-1].EndHeight = i
							validatorCommitInfo.CommitInfos[slotCount-1].CommitCount++
						} else {
							validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
								Slot:        slot,
								StartHeight: i,
								EndHeight:   i,
								CommitCount: 1,
							})
							validatorCommitInfo.SlotCount++
						}
					}
				}
			}

			// 1) Proposer count per validator
			outputProposerFile, _ := os.OpenFile(fmt.Sprintf("proposer-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputProposerFile.Close()

			writerProposer := csv.NewWriter(outputProposerFile)
			outputProposer := []string{
				"Height", "Proposer Address", "TX Count",
			}
			writeFile(writerProposer, outputProposer, outputProposerFile.Name())

			for _, p := range proposerMap {

				outputProposer := []string{
					fmt.Sprint(p.Height),
					fmt.Sprint(p.ProposerAddress),
					fmt.Sprint(p.TxCount),
				}

				writeFile(writerProposer, outputProposer, outputProposerFile.Name())
			}

			fmt.Println("Done! check the output files on current dir : ", outputProposerFile.Name())

			// 2) Proposer tx count per validator
			outputProposerTxFile, _ := os.OpenFile(fmt.Sprintf("proposer-tx-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputProposerFile.Close()

			writerProposerTx := csv.NewWriter(outputProposerTxFile)
			outputProposerTx := []string{
				"Proposer Address", "Proposing Count", "TX Count",
			}
			writeFile(writerProposerTx, outputProposerTx, outputProposerTxFile.Name())

			for _, p := range proposerTxMap {

				outputProposerTx := []string{
					fmt.Sprint(p.ProposerAddress),
					fmt.Sprint(p.ProposingCount),
					fmt.Sprint(p.TxCount),
				}

				writeFile(writerProposerTx, outputProposerTx, outputProposerTxFile.Name())
			}

			fmt.Println("Done! check the output files on current dir : ", outputProposerTxFile.Name())

			// 3) Commit count per validator
			outputFile, _ := os.OpenFile(fmt.Sprintf("data-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputFile.Close()

			writer := csv.NewWriter(outputFile)
			output := []string{
				"Validator Address", "Slot Count", "Slot", "Start Height", "End Height", "Commit Count", "Block Count", "Missed Commit",
			}
			writeFile(writer, output, outputFile.Name())

			for _, v := range validatorMap {

				for _, cv := range v.CommitInfos {

					blockCount := cv.EndHeight - cv.StartHeight + 1
					missedCommit := blockCount - cv.CommitCount

					output := []string{
						v.ValidatorAddress,
						fmt.Sprint(v.SlotCount),
						fmt.Sprint(cv.Slot),
						fmt.Sprint(cv.StartHeight),
						fmt.Sprint(cv.EndHeight),
						fmt.Sprint(cv.CommitCount),
						fmt.Sprint(blockCount),
						fmt.Sprint(missedCommit),
					}

					writeFile(writer, output, outputFile.Name())
				}
			}

			fmt.Println("Done! check the output files on current dir : ", outputFile.Name())
			return nil
		},
	}
	return cmd
}

// RPCParserCmd
// go run main.go https://rpc-juno-old-archive.cosmoapi.com 2017467 2578097
// Total 560,000 blocks
// Throughput 120,000 blocks/h
// Estimation 4.6 hours
func RPCParserCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:  "blockparser rpc [rpc url] [start-height] [end-height]",
		Args: cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {

			startHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}

			endHeight, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			rpcUrl := args[1]

			fmt.Println("RPC URL : ", rpcUrl)
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)

			validatorMap := make(map[string]*ValidatorCommitInfo)
			emptyCommitMap := make(map[int]*EmptyCommit)

			// Gather data from RPC
			// - Commit count per validator
			for i := startHeight; i <= endHeight; i++ {
				if i%10000 == 0 {
					t := time.Now()
					fmt.Println(i, " - ", t)
				}

				blockUrl := rpcUrl + "/block?height=" + strconv.FormatInt(i, 10)
				res, err := http.Get(blockUrl)
				if err != nil {
					panic(err)
				}

				body, err := ioutil.ReadAll(res.Body)
				if err != nil {
					panic(err)
				}

				jsonString := string(body)

				rpcBlockData := RPCBlockData{}
				json.Unmarshal([]byte(jsonString), &rpcBlockData)

				blockCommit := &rpcBlockData.Result.Block.LastCommit

				for slot, item := range blockCommit.Signatures {

					// if no signature in the slot
					if item.ValidatorAddress == "" {

						_, ok := emptyCommitMap[slot]
						if !ok {
							emptyCommit := EmptyCommit{
								Slot: slot,
							}
							emptyCommitMap[slot] = &emptyCommit
						}

						emptyCommit := emptyCommitMap[slot]
						emptyCommit.Heights = append(emptyCommit.Heights, i)

						continue
					}

					_, ok := validatorMap[item.ValidatorAddress]
					if !ok {
						validatorCommitInfo := ValidatorCommitInfo{
							ValidatorAddress: item.ValidatorAddress,
							SlotCount:        1,
						}

						validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
							Slot:        slot,
							StartHeight: i,
							EndHeight:   i,
							CommitCount: 1,
						})

						validatorMap[item.ValidatorAddress] = &validatorCommitInfo
					} else {
						validatorCommitInfo := validatorMap[item.ValidatorAddress]
						slotCount := validatorCommitInfo.SlotCount

						if slot == validatorCommitInfo.CommitInfos[slotCount-1].Slot {
							validatorCommitInfo.CommitInfos[slotCount-1].CommitCount++
							validatorCommitInfo.CommitInfos[slotCount-1].EndHeight = i
						} else {
							validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
								Slot:        slot,
								StartHeight: i,
								EndHeight:   i,
								CommitCount: 1,
							})
							validatorCommitInfo.SlotCount++
						}
					}
				}
			}

			outputFile, _ := os.OpenFile(fmt.Sprintf("data-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputFile.Close()

			writer := csv.NewWriter(outputFile)
			output := []string{
				"Validator Address", "Slot Count", "Slot", "Start Height", "End Height", "Commit Count", "Block Count", "Missed Commit",
			}
			writeFile(writer, output, outputFile.Name())

			for _, v := range validatorMap {

				for _, cv := range v.CommitInfos {

					blockCount := cv.EndHeight - cv.StartHeight + 1
					missedCommit := blockCount - cv.CommitCount

					output := []string{
						v.ValidatorAddress,
						fmt.Sprint(v.SlotCount),
						fmt.Sprint(cv.Slot),
						fmt.Sprint(cv.StartHeight),
						fmt.Sprint(cv.EndHeight),
						fmt.Sprint(cv.CommitCount),
						fmt.Sprint(blockCount),
						fmt.Sprint(missedCommit),
					}

					writeFile(writer, output, outputFile.Name())
				}
			}

			fmt.Println("Done! check the output files on current dir : ", outputFile.Name())
			return nil
		},
	}
	return cmd
}

func ConsensusParserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "blockparser consensus [rpc url]",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {

			rpcUrl := args[1]

			fmt.Println("RPC URL : ", rpcUrl)

			res, err := http.Get(rpcUrl)
			if err != nil {
				panic(err)
			}

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				panic(err)
			}

			jsonString := string(body)

			consensusStateData := ConsensusStateInfo{}
			json.Unmarshal([]byte(jsonString), &consensusStateData)

			var lastesIndex int = len(consensusStateData.Result.RoundState.HeightVoteSet) - 2
			fmt.Println(consensusStateData.Result.RoundState.HeightVoteSet[lastesIndex].Round)

			prevoteMap := make(map[string]int)

			for _, item := range consensusStateData.Result.RoundState.HeightVoteSet[lastesIndex].Prevotes {

				var key string = ""
				if item == "nil-Vote" {
					key = "nil-Vote"

				} else {
					s := strings.Split(item, " ")
					key = s[2]
				}

				fmt.Println(key)
				_, ok := prevoteMap[key]
				if !ok {
					prevoteMap[key] = 1
				} else {
					prevoteMap[key] += 1
				}
			}

			for key, value := range prevoteMap {
				fmt.Println(key, value)
			}

			return nil
		},
	}
	return cmd
}

func writeFile(w *csv.Writer, result []string, fileName string) {
	if err := w.Write(result); err != nil {
		return
	}
	w.Flush()

	if err := w.Error(); err != nil {
		return
	}
}
