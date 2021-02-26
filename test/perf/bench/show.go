package main

import (
	"context"
	"fmt"
	"log"
	"os"

	v3 "github.com/roguesoftware/etcd/clientv3"

	"github.com/spf13/cobra"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show key [end-range]",
	Short: "Show range of k/v from cluster",

	Run: showFunc,
}

func init() {
	rootCmd.AddCommand(showCmd)
}

func showFunc(cmd *cobra.Command, args []string) {
	if len(args) > 2 || len(args) == 0 {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	var r KeyRange

	switch {
	case len(args) == 1:
		r = KeyRange{key: args[0]}
	case len(args) == 2:
		r = KeyRange{key: args[0], end: args[1]}
	}

	c := mustCreateConn()

	opts := []v3.OpOption{}
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
	for _, kv := range gr.Kvs {
		fmt.Println(string(kv.Key))
	}

	c.Close()
}
