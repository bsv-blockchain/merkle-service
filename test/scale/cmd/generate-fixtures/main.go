package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	seed := flag.Int64("seed", 42, "deterministic seed for hash generation")
	instances := flag.Int("instances", 50, "number of Arcade instances")
	txidsPerInstance := flag.Int("txids-per-instance", 1000, "txids per Arcade instance")
	subtrees := flag.Int("subtrees", 50, "number of subtrees per block")
	txidsPerSubtree := flag.Int("txids-per-subtree", 1024, "txids per subtree")
	outDir := flag.String("out", "testdata", "output directory for fixtures")
	flag.Parse()

	totalTxids := *instances * *txidsPerInstance
	totalSubtreeTxids := *subtrees * *txidsPerSubtree

	if totalTxids != totalSubtreeTxids {
		fmt.Fprintf(os.Stderr, "ERROR: instances*txids-per-instance (%d) must equal subtrees*txids-per-subtree (%d)\n",
			totalTxids, totalSubtreeTxids)
		os.Exit(1)
	}

	fmt.Printf("Generating fixtures:\n")
	fmt.Printf("  Seed:              %d\n", *seed)
	fmt.Printf("  Arcade instances:  %d\n", *instances)
	fmt.Printf("  Txids/instance:    %d\n", *txidsPerInstance)
	fmt.Printf("  Subtrees:          %d\n", *subtrees)
	fmt.Printf("  Txids/subtree:     %d\n", *txidsPerSubtree)
	fmt.Printf("  Total txids:       %d\n", totalTxids)
	fmt.Printf("  Output dir:        %s\n", *outDir)

	if err := generateAll(*seed, *instances, *txidsPerInstance, *subtrees, *txidsPerSubtree, *outDir); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Done.")
}
