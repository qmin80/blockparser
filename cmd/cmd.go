package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/store"
	tmdb "github.com/tendermint/tm-db"
)

func PebbleBlockParserCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:  "blockparser pebble [chain-dir] [start-height] [end-height]",
		Args: cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			startHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}

			endHeight, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			db, err := tmdb.NewDB("data/blockstore", "pebbledb", dir)
			if err != nil {
				panic(err)
			}
			defer db.Close()

			stateDB, err := tmdb.NewDB("data/state", "pebbledb", dir)
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

			validatorMap := make(map[string]*ValidatorCommitInfo)
			emptyCommitMap := make(map[int]*EmptyCommit)
			proposerMap := make(map[int]*ProposerInfo)
			proposerTxMap := make(map[string]*ProposerTxInfo)

			for i := startHeight; i <= endHeight; i++ {

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

				b, err := json.Marshal(blockStore.LoadBlockCommit(i))
				if err != nil {
					panic(err)
				}

				jsonString := string(b)
				var blockCommit = BlockCommit{}
				json.Unmarshal([]byte(jsonString), &blockCommit)

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

			// proposerTxMap
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

			outputFile, _ := os.OpenFile(fmt.Sprintf("data-%d-%d.csv", startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			defer outputFile.Close()

			writer := csv.NewWriter(outputFile)
			output := []string{
				"Validator Address", "Slot Count", "Slot", "Start Height", "End Height", "Commit Count", "Block Count", "Missed Commit",
			}
			writeFile(writer, output, outputFile.Name())

			for _, v := range validatorMap {

				for _, cv := range v.CommitInfos {

					blockCount := cv.EndHeight - cv.StartHeight
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

func writeFile(w *csv.Writer, result []string, fileName string) {
	if err := w.Write(result); err != nil {
		return
	}
	w.Flush()

	if err := w.Error(); err != nil {
		return
	}
}
