package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/roguesoftware/etcd/clientv3"
	"github.com/spf13/cobra"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/protobuf/proto"
	"gopkg.in/cheggaaa/pb.v1"
)

var nodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Create node objects",

	Run: nodeFunc,
}

var (
	createIndices bool

	totalNodes int
)

func init() {
	rootCmd.AddCommand(nodesCmd)
	nodesCmd.Flags().StringVar(&trustDomain, "trust-domain", "domain.test", "Trust domain name")
	nodesCmd.Flags().IntVar(&total, "total", 100, "Total number of attested nodes to create")
	nodesCmd.Flags().BoolVar(&createIndices, "index", false, "create node index entries")
	nodesCmd.Flags().BoolVar(&progress, "progress", false, "show progress bar")
}

func nodeFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	// Seed math.rand with cryptographically random number and loads word file
	randInit()

	// Node paths contain three parts; get cube root for count and add one for
	// additional SPIFFE IDs as parallel operations may not divide equally into total
	count := int(math.Ceil(math.Cbrt(float64(total)))) + 1

	// Get two distinctly unique word lists to build random node names
	regions, clusters, _ := getThreeWordLists(count)

	// Create a lit of attested node SPIFFE IDs with random "region" and "cluster" names
	spiffeIds := []string{}
	for r := 0; r < count; r++ {
		region := regions[r]
		for c := 0; c < count; c++ {
			cluster := clusters[c]
			for n := 0; n < count; n++ {
				s := fmt.Sprintf("spiffe://spire/agent/%s/%s/%s/node%d", trustDomain, region, cluster, n)
				spiffeIds = append(spiffeIds, s)
			}
		}
	}

	// Get the requested number of client connections
	clients := mustCreateClients(totalClients, totalConns)
	nodesPerClient := total/len(clients) + 1

	if len(spiffeIds) < nodesPerClient*len(clients) {
		fmt.Printf("Not enough Spiffe IDs (%d) for %d items per %d clients", len(spiffeIds), nodesPerClient, len(clients))
		os.Exit(1)
	}

	// Progress bar if requested
	bar = pb.New(total)
	if progress {
		bar.Format("==>")
		bar.Start()
	}
	start := time.Now().UnixNano()

	for i := range clients {
		wg.Add(1)
		go func(clientNumber, count int) {
			defer wg.Done()
			c := &etcdClient{c: clients[clientNumber]}
			sids := spiffeIds[count*clientNumber : count*(clientNumber+1)]
			for j := 0; j < count; j++ {
				err := c.createNode(sids[j], createIndices)
				if !ok(err) {
					return
				}

				if progress {
					bar.Increment()
				}

				mu.Lock()
				totalNodes++
				tn := totalNodes
				mu.Unlock()

				if tn >= total {
					break
				}
			}
		}(i, nodesPerClient)
	}

	wg.Wait()
	finish := time.Now().UnixNano()
	if progress {
		bar.Finish()
	}

	delta := (float64(finish) - float64(start)) / 1000000000.0
	fmt.Printf("%d nodes written in %.3f sec (%.3f ops/sec)\n", totalNodes, delta, float64(totalNodes)/delta)
}

func (ec *etcdClient) createNode(sid string, idx bool) error {
	selectors := []*common.Selector{
		{Type: randString(10), Value: randString(32)},
		{Type: randString(10), Value: randString(32)},
		{Type: randString(10), Value: randString(32)},
		{Type: randString(10), Value: randString(32)},
	}

	n := &common.AttestedNode{
		SpiffeId:            sid,
		AttestationDataType: randString(8),
		CertNotAfter:        time.Now().Add(time.Hour * 12).Unix(),
		CertSerialNumber:    fmt.Sprintf("%d%d", rand.Intn(math.MaxInt64), rand.Intn(math.MaxInt64)),
		NewCertNotAfter:     time.Now().Add(time.Hour * 24).Unix(),
		NewCertSerialNumber: fmt.Sprintf("%d%d", rand.Intn(math.MaxInt64), rand.Intn(math.MaxInt64)),
		Selectors:           selectors,
	}

	k := store.NodeKey(n.SpiffeId)
	v, err := proto.Marshal(n)
	if err != nil {
		return err
	}

	ops := []clientv3.Op{}
	ops = append(ops, clientv3.OpPut(k, string(v)))

	// Add index keys if requested
	if idx {
		ops = append(ops, clientv3.OpPut(store.NodeAdtKey(n.SpiffeId, n.AttestationDataType), ""))
		ops = append(ops, clientv3.OpPut(store.NodeBanKey(n.SpiffeId, n.CertSerialNumber), ""))

		sel := &datastore.NodeSelectors{SpiffeId: n.SpiffeId, Selectors: n.Selectors}
		v, err = proto.Marshal(sel)
		if err != nil {
			return err
		}
		ops = append(ops, clientv3.OpPut(store.NodeExpKey(n.SpiffeId, n.CertNotAfter), string(v)))

		for _, sel := range n.Selectors {
			ops = append(ops, clientv3.OpPut(store.NodeSelKey(n.SpiffeId, sel), ""))
		}
	}

	// Send the transaction to the etcd cluster
	_, err = ec.c.Txn(context.TODO()).Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}
