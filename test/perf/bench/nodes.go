package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

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

	// Node paths will contain three parts; get the cube root for counting and add one for
	// additional SPIFFE IDs as parallel operations don't always divide equally into the total
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
	fmt.Printf("%d SpiffeIDs\n", len(spiffeIds))

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
				_, err := c.createNode(sids[j], createIndices)
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
	fmt.Printf("%d nodes written in %.3f sec\n", totalNodes, (float64(finish)-float64(start))/1000000000.0)
}

func (ec *etcdClient) createNode(sid string, idx bool) (*common.AttestedNode, error) {
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
		return nil, err
	}

	_, err = ec.c.Put(context.Background(), k, string(v))
	if err != nil {
		return nil, err
	}

	if !idx {
		return n, nil
	}

	k = store.NodeAdtKey(n.SpiffeId, n.AttestationDataType)
	_, err = ec.c.Put(context.Background(), k, "")
	if err != nil {
		return nil, err
	}

	k = store.NodeBanKey(n.SpiffeId, n.CertSerialNumber)
	_, err = ec.c.Put(context.Background(), k, "")
	if err != nil {
		return nil, err
	}

	sel := &datastore.NodeSelectors{SpiffeId: n.SpiffeId, Selectors: n.Selectors}
	v, err = proto.Marshal(sel)
	if err != nil {
		return nil, err
	}
	k = store.NodeExpKey(n.SpiffeId, n.CertNotAfter)
	_, err = ec.c.Put(context.Background(), k, string(v))
	if err != nil {
		return nil, err
	}

	for _, sel := range n.Selectors {
		k = store.NodeSelKey(n.SpiffeId, sel)
		_, err = ec.c.Put(context.Background(), k, string(v))
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}
