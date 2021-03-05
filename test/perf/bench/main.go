package main

import (
	"fmt"
	"os"
)

func main() {
	// cf, err := os.Create("pcpu")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// pprof.StartCPUProfile(cf)
	// defer pprof.StopCPUProfile()

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
}
