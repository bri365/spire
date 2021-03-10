package main

import (
	"context"
	"fmt"
	"log"
	"os"

	v3 "github.com/roguesoftware/etcd/clientv3"

	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete key [end-range] [--all [--nocache]]",
	Short: "Delete one or a range of k/v from cluster",

	Run: deleteFunc,
}

var (
	nocache bool
)

func init() {
	rootCmd.AddCommand(deleteCmd)
	deleteCmd.Flags().BoolVar(&all, "all", false, "'true' to delete all keys")
	deleteCmd.Flags().BoolVar(&nocache, "nocache", false, "'true' to ignore cache objects")
	deleteCmd.Flags().BoolVar(&show, "show", false, "'true' to show all deleted keys")
}

func deleteFunc(cmd *cobra.Command, args []string) {
	if len(args) > 2 || (len(args) == 0 && !all) {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	if nocache && !all {
		fmt.Fprintln(os.Stderr, "cache requires all to be true")
		os.Exit(1)
	}

	// ranges of keys to delete
	var ranges []KeyRange

	switch {
	case all && nocache:
		// Delete all keys except cache objects
		ranges = []KeyRange{
			{key: string([]byte{0}), end: "B|"},
			{key: "B}", end: "E|"},
			{key: "E}", end: "H|"},
			{key: "H}", end: "N|"},
			{key: "N}", end: "T|"},
			{key: "T}", end: string([]byte{0})},
		}
	case all:
		// Delete all keys
		ranges = []KeyRange{
			{key: string([]byte{0}), end: string([]byte{0})},
		}
	case len(args) == 1:
		ranges = []KeyRange{{key: args[0]}}
	case len(args) == 2:
		ranges = []KeyRange{{key: args[0], end: args[1]}}
	}

	c := mustCreateConn()
	var total int64

	for _, r := range ranges {
		// set the range
		opts := []v3.OpOption{}
		switch {
		case r.end != "":
			opts = append(opts, v3.WithRange(r.end))
		case r.prefix:
			opts = append(opts, v3.WithPrefix())
		}

		if show {
			gr, err := c.Get(context.Background(), r.key, opts...)
			if err != nil {
				log.Fatal(err)
			}

			if gr.Count > 0 {
				fmt.Printf("Deleting %d keys\n", gr.Count)
			}

			for _, k := range gr.Kvs {
				fmt.Println(string(k.Key))
			}
		}

		dr, err := c.Delete(context.Background(), r.key, opts...)
		if err != nil {
			log.Fatal(err)
		}

		total += dr.Deleted
	}

	fmt.Printf("Deleted %d keys\n", total)

	c.Close()
}
