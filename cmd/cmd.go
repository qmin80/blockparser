package cmd

import (
	"encoding/json"
	"encoding/csv"
	"net/http"
	"fmt"
	"strconv"
	"time"
	"os"
	"io/ioutil"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/store"
)

type BlockCommit struct {
	Height  int `json:"height"`
	Round   int `json:"round"`
	BlockID struct {
		Hash  string `json:"hash"`
		Parts struct {
			Total int    `json:"total"`
			Hash  string `json:"hash"`
		} `json:"parts"`
	} `json:"block_id"`
	Signatures []struct {
		BlockIDFlag      int       `json:"block_id_flag"`
		ValidatorAddress string    `json:"validator_address"`
		Timestamp        time.Time `json:"timestamp"`
		Signature        string    `json:"signature"`
	} `json:"signatures"`
}

type ValidatorCommitInfo struct {
	ValidatorAddress string    `json:"validator_address"`
	SlotCount  int `json:"slot_count"`
	CommitInfos	[]CommitInfo    `json:"commit_infos"`
}

type ProposerInfo struct {
	Height  int64 `json:"height"`
	ProposerAddress string    `json:"proposer_address"`
	TxCount  int `json:"tx_count"`
}

type CommitInfo struct {
	Slot  int `json:"slot"`
	StartHeight  int64 `json:"start_height"`
	EndHeight  int64 `json:"end_height"`
	CommitCount   int64 `json:"commit_count"`
} 

type EmptyCommit struct {
	Slot	int `json:"slot"`	
	Heights  []int64 `json:"height"`	
}

type RPCBlockData struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  struct {
		BlockID struct {
			Hash  string `json:"hash"`
			Parts struct {
				Total int    `json:"total"`
				Hash  string `json:"hash"`
			} `json:"parts"`
		} `json:"block_id"`
		Block struct {
			Header struct {
				Version struct {
					Block string `json:"block"`
				} `json:"version"`
				ChainID     string    `json:"chain_id"`
				Height      string    `json:"height"`
				Time        time.Time `json:"time"`
				LastBlockID struct {
					Hash  string `json:"hash"`
					Parts struct {
						Total int    `json:"total"`
						Hash  string `json:"hash"`
					} `json:"parts"`
				} `json:"last_block_id"`
				LastCommitHash     string `json:"last_commit_hash"`
				DataHash           string `json:"data_hash"`
				ValidatorsHash     string `json:"validators_hash"`
				NextValidatorsHash string `json:"next_validators_hash"`
				ConsensusHash      string `json:"consensus_hash"`
				AppHash            string `json:"app_hash"`
				LastResultsHash    string `json:"last_results_hash"`
				EvidenceHash       string `json:"evidence_hash"`
				ProposerAddress    string `json:"proposer_address"`
			} `json:"header"`
			Data struct {
				Txs []string `json:"txs"`
			} `json:"data"`
			Evidence struct {
				Evidence []interface{} `json:"evidence"`
			} `json:"evidence"`
			LastCommit struct {
				Height  string `json:"height"`
				Round   int    `json:"round"`
				BlockID struct {
					Hash  string `json:"hash"`
					Parts struct {
						Total int    `json:"total"`
						Hash  string `json:"hash"`
					} `json:"parts"`
				} `json:"block_id"`
				Signatures []struct {
					BlockIDFlag      int       `json:"block_id_flag"`
					ValidatorAddress string    `json:"validator_address"`
					Timestamp        time.Time `json:"timestamp"`
					Signature        string    `json:"signature"`
				} `json:"signatures"`
			} `json:"last_commit"`
		} `json:"block"`
	} `json:"result"`
}

func BlockParserCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:  "blockparser [chain-dir] [start-height] [end-height]",
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
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

			validatorMap := make(map[string]*ValidatorCommitInfo)
			emptyCommitMap := make(map[int]*EmptyCommit)
			proposerMap := make(map[int]*ProposerInfo)

			for i := startHeight; i <= endHeight; i++ {

				block := blockStore.LoadBlock(i)
				proposerInfo := ProposerInfo{
					Height: i,
					ProposerAddress: fmt.Sprint(block.ProposerAddress),
					TxCount: len(block.Txs),
				}
				proposerMap[int(i)] = &proposerInfo
				
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
							SlotCount: 1,
						}
						
						validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
							Slot: slot,
							StartHeight: i,
							EndHeight: i,
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
								Slot: slot,
								StartHeight: i,
								EndHeight: i,
								CommitCount: 1,
							})							
							validatorCommitInfo.SlotCount++
						}
					}
				}
			}

			outputProposerFile, _ := os.OpenFile(fmt.Sprintf("proposer-%d-%d.csv",startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
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


			outputFile, _ := os.OpenFile(fmt.Sprintf("data-%d-%d.csv",startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
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


// RPCParserCmd 
// go run main.go https://rpc-juno-old-archive.cosmoapi.com 2017467 2578097
// Total 560,000 blocks 
// Throughput 120,000 blocks/h
// Estimation 4.6 hours
func RPCParserCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:  "blockparser [rpc url] [start-height] [end-height]",
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {

			startHeight, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("parse start-Height: %w", err)
			}

			endHeight, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse end-Height: %w", err)
			}

			rpcUrl := args[0]
			
			fmt.Println("RPC URL : ", rpcUrl)
			fmt.Println("Input Start Height :", startHeight)
			fmt.Println("Input End Height :", endHeight)

			validatorMap := make(map[string]*ValidatorCommitInfo)
			emptyCommitMap := make(map[int]*EmptyCommit)

			for i := startHeight; i <= endHeight; i++ {
				if i % 10000 == 0 {
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
				// fmt.Println(*blockCommit)

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
							SlotCount: 1,
						}
						
						validatorCommitInfo.CommitInfos = append(validatorCommitInfo.CommitInfos, CommitInfo{
							Slot: slot,
							StartHeight: i,
							EndHeight: i,
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
								Slot: slot,
								StartHeight: i,
								EndHeight: i,
								CommitCount: 1,
							})							
							validatorCommitInfo.SlotCount++
						}
					}
				}
			}
			
			outputFile, _ := os.OpenFile(fmt.Sprintf("data-%d-%d.csv",startHeight, endHeight), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
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

