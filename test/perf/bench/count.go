package main

import (
	"context"
	"fmt"
	"log"
	"os"

	v3 "github.com/roguesoftware/etcd/clientv3"

	"github.com/spf13/cobra"
)

var countCmd = &cobra.Command{
	Use:   "count key [end-range]",
	Short: "Count one or a range of keys in the cluster",

	Run: countFunc,
}

func init() {
	rootCmd.AddCommand(countCmd)
	countCmd.Flags().BoolVar(&all, "all", false, "count all keys")
}

func countFunc(cmd *cobra.Command, args []string) {
	if len(args) > 2 || (len(args) == 0 && !all) {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	var r KeyRange

	switch {
	case all:
		fmt.Println("Counting all keys")
		r = KeyRange{key: string([]byte{0}), end: string([]byte{0})}
	case len(args) == 1:
		r = KeyRange{key: args[0]}
	case len(args) == 2:
		r = KeyRange{key: args[0], end: args[1]}
	}

	c := mustCreateConn()

	opts := []v3.OpOption{v3.WithCountOnly()}
	switch {
	case r.end != "":
		opts = append(opts, v3.WithRange(r.end))
	case r.prefix:
		opts = append(opts, v3.WithPrefix())
	}

	gr, err := c.Get(context.Background(), r.key, opts...)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d keys\n", gr.Count)

	c.Close()
}
