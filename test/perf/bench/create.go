package main

// Create bundles, nodes, and entries in the store for scale testing.
//
// Node IDs are formatted as spiffe://<trust domain>/spire/agent/<cluster>/<node UID>
//
// Node selectors include:
//   k8s_psat:cluster:<cluster>
//   k8s_psat:agent_ns:spire
//   k8s_psat:agent_sa:spire-agent
//   k8s_psat:agent_pod_name:spire-agent-<5 digit random string of lowercase alpha and numbers>
//   k8s_psat:agent_pod_uid:<UUID4>
//   k8s_psat:agent_pod_label:version:0.12.2
//   k8s_psat:agent_node_ip:172.16.<cluster unique>
//   k8s_psat:agent_node_name:node<node number>.<cluster>.<region>.<trust domain>
//   k8s_psat:agent_node_uid:<UUID4>
//   k8s_psat:agent_node_label:region:<region>
//   k8s_psat:agent_node_label:arch:<aarch64|amd64>
//   k8s_psat:agent_node_label:bw:<1|5|10|25|50|100>
//   k8s_psat:agent_node_label:gpu:<true|false>
//   k8s_psat:agent_node_label:pmem:<true|false>
//
// The number of regions and clusters is based on the total number of nodes unless overridden
//
// | Total nodes | Regions | Clusters | Nodes/Cluster |
// +-------------+---------+----------+---------------+
// |        <100 |       1 |      1-5 |         10-20 |
// |       <1000 |     1-3 |     5-10 |         20-33 |
// |     <10,000 |     3-6 |    10-40 |         30-40 |
// |    <100,000 |     6-8 |    40-70 |        40-180 |
// |  <1,000,000 |    8-10 |   70-100 |      180-1000 |
// +-------------+---------+----------+---------------+
//

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/url"
	"os"
	"time"

	"github.com/roguesoftware/etcd/clientv3"
	"github.com/spf13/cobra"
	"github.com/spiffe/spire/pkg/common/pemutil"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/store"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/proto/spire/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/cheggaaa/pb.v1"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create bundle, entry, and node objects",

	Run: createFunc,
}

var (
	createHostgroups bool
	createIndices    bool
	random           bool

	certNotAfter    int64
	newCertNotAfter int64

	totalBundles   int
	totalNodes     int
	totalWorkloads int

	certSerialNumber    string
	newCertSerialNumber string

	clusterFile string
	regionFile  string

	clusters   []string
	regions    []string
	archValues = []string{"aarch64", "amd64"}
	gpuValues  = []string{"nogpu", "cuda"}
	// bwValues   = []string{"1", "5", "10", "25", "50", "100"}

	nodesByRegion = map[string][]*common.AttestedNode{}
	archByRegion  = map[string]map[string][]*common.AttestedNode{}
	gpuByRegion   = map[string]map[string][]*common.AttestedNode{}

	federatedBundleIDs = []string{}
)

func init() {
	rootCmd.AddCommand(createCmd)
	createCmd.Flags().StringVar(&trustDomain, "trust-domain", "domain.test", "trust domain name")
	createCmd.Flags().StringVar(&clusterFile, "cluster-file", "clusters.txt", "filename for cluster names")
	createCmd.Flags().StringVar(&regionFile, "region-file", "regions.txt", "filename for region names")

	createCmd.Flags().IntVar(&totalBundles, "bundles", 0, "number of federated bundles to create")
	createCmd.Flags().IntVar(&totalNodes, "nodes", 10, "number of attested nodes to create")
	createCmd.Flags().IntVar(&totalWorkloads, "workloads", 10, "number of workloads to create")

	createCmd.Flags().BoolVar(&createHostgroups, "host-groups", false, "create a host group entry per node")
	createCmd.Flags().BoolVar(&createIndices, "index", false, "create node index entries")
	createCmd.Flags().BoolVar(&progress, "progress", false, "show progress bar")
	createCmd.Flags().BoolVar(&random, "random", false, "seed math/rand with crypto/rand")
}

// createFunc implements the bench create command, creating bundles, entries, and nodes in the store.
func createFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	if random {
		initRand()
	}

	// Get region and cluster names based on total node count
	regionCount, clusterCount := regionsAndClusters(totalNodes)
	regions = mustRandomFileStrings(regionFile, regionCount)
	clusters = mustRandomFileStrings(clusterFile, clusterCount)

	// Set nodes per cluster, rounding up to ensure there are sufficient to meet total request
	nodeCount := totalNodes / (regionCount * clusterCount)
	if regionCount*clusterCount*nodeCount < totalNodes {
		nodeCount++
	}

	// Set up client connections and ensure enough nodes per client to meet total request
	clients := mustCreateClients(totalClients, totalConns)
	nodesPerClient := int(math.Ceil(float64(totalNodes) / float64(len(clients))))
	clientTotal := nodesPerClient * len(clients)
	rcnTotal := regionCount * clusterCount * nodeCount
	if rcnTotal < clientTotal {
		nodeCount = nodeCount + (clientTotal-rcnTotal)/(regionCount*clusterCount) + 1
	}

	// Create empty node maps
	for _, r := range regions {
		archByRegion[r] = map[string][]*common.AttestedNode{}
		for _, a := range archValues {
			archByRegion[r][a] = []*common.AttestedNode{}
		}
		gpuByRegion[r] = map[string][]*common.AttestedNode{}
		for _, g := range gpuValues {
			gpuByRegion[r][g] = []*common.AttestedNode{}
		}
	}

	// Create a list of nodes to store
	nodes := createNodes(regionCount, clusterCount, nodeCount)

	if len(nodes) < nodesPerClient*len(clients) {
		fmt.Printf("Insufficient nodes (%d) for %d items per %d clients", len(nodes), nodesPerClient, len(clients))
		os.Exit(1)
	}

	fmt.Printf("Storing %d nodes across %d clusters with %d nodes each\n", totalNodes, regionCount*clusterCount, nodesPerClient)

	// Store the nodes
	storeNodes(clients, nodes, nodesPerClient)

	// Create federated bundles
	bundles := createFederatedBundles(totalBundles)

	// Store the bundles
	storeFederatedBundles(clients[0], bundles)

	// Create lists of hostgroup entries
	entries := createHostgroupEntries()

	// Create lists of workload entries for each hostgroup
	entries = append(entries, createWorkloadEntries(totalWorkloads, bundles)...)

	// Store the entries
	storeEntries(clients, entries)
}

// createFederatedBundles
func createFederatedBundles(total int) []*types.Bundle {
	bundles := []*types.Bundle{}
	if total < 1 {
		return bundles
	}

	f, err := os.Open("ca.pem")
	if err != nil {
		fmt.Println("Error opening ca.pem")
		os.Exit(1)
	}
	defer f.Close()

	pem, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println("Error reading ca.pem")
		os.Exit(1)
	}

	tds := mustRandomFileStrings("trustdomains.txt", total)
	for _, td := range tds {
		rootCAs, err := pemutil.ParseCertificates(pem)
		if err != nil {
			fmt.Printf("unable to parse ca.pem: %v", err)
			os.Exit(1)
		}

		id := fmt.Sprintf("spiffe://%s", td)
		b := &types.Bundle{TrustDomain: id}
		for _, rootCA := range rootCAs {
			b.X509Authorities = append(b.X509Authorities, &types.X509Certificate{Asn1: rootCA.Raw})
		}

		bundles = append(bundles, b)
		federatedBundleIDs = append(federatedBundleIDs, id)
	}

	return bundles
}

func storeFederatedBundles(client *clientv3.Client, all []*types.Bundle) {
	// Store the entries in parallel across the client connections
	start := time.Now().UnixNano()
	for _, b := range all {
		k := store.BundleKey(b.TrustDomain)
		v, err := proto.Marshal(b)
		if err != nil {
			fmt.Printf("unable to marshal bundle %s: %v", b.TrustDomain, err)
			os.Exit(1)
		}

		_, err = client.Put(context.TODO(), k, string(v))
		if err != nil {
			fmt.Printf("unable to store bundle %s: %v", b.TrustDomain, err)
			os.Exit(1)
		}

		if debug {
			fmt.Printf("Stored %s\n", k)
		}
	}
	finish := time.Now().UnixNano()
	delta := (float64(finish) - float64(start)) / 1000000000.0
	fmt.Printf("%d bundles written in %.3f sec (%.3f ops/sec)\n", len(all), delta, float64(len(all))/delta)

	return
}

// createWorkloadEntries() {}
func createWorkloadEntries(total int, bundles []*types.Bundle) []*common.RegistrationEntry {
	entries := []*common.RegistrationEntry{}
	for i := 0; i < total; i++ {
		region := regions[rand.Intn(len(regions))]
		entry := &common.RegistrationEntry{
			EntryId:  mustUUID(),
			SpiffeId: fmt.Sprintf("spiffe://%s/%s/workload%d", trustDomain, region, i),
			Selectors: []*common.Selector{
				{Type: "ns", Value: randString(lowerNum, 12)},
				{Type: "app", Value: randString(lowerNum, 12)},
			},
		}

		// Set a random parent from host groups
		switch rand.Intn(5) {
		case 4:
			arch := archValues[rand.Intn(len(archValues))]
			entry.ParentId = fmt.Sprintf("spiffe://%s/%s/%s", trustDomain, region, arch)
		case 3:
			gpu := gpuValues[rand.Intn(len(gpuValues))]
			entry.ParentId = fmt.Sprintf("spiffe://%s/%s/%s", trustDomain, region, gpu)
		default:
			entry.ParentId = fmt.Sprintf("spiffe://%s/%s", trustDomain, region)
		}

		if len(federatedBundleIDs) > 0 {
			// Set random federated bundles for 20% of entries
			if rand.Intn(5) == 0 {
				count := rand.Intn(rand.Intn(rand.Intn(rand.Intn(len(federatedBundleIDs)+1)+1)+1)+1) + 1
				if count > len(federatedBundleIDs) {
					count = len(federatedBundleIDs)
				}
				indexSet := map[int]struct{}{}
				for {
					indexSet[rand.Intn(len(federatedBundleIDs))] = struct{}{}
					if len(indexSet) >= count {
						break
					}
				}
				for i := range indexSet {
					entry.FederatesWith = append(entry.FederatesWith, federatedBundleIDs[i])
				}
			}
		}

		entries = append(entries, entry)
	}

	return entries
}

// createHostgroupEntries - 1 per region, 1 per architecture type per region, 1 per gpu type per region
func createHostgroupEntries() []*common.RegistrationEntry {
	entries := []*common.RegistrationEntry{}
	start := time.Now().UnixNano()

	for region, nodes := range nodesByRegion {
		// Host group
		entries = append(entries, &common.RegistrationEntry{
			EntryId:  mustUUID(),
			SpiffeId: fmt.Sprintf("spiffe://%s/%s", trustDomain, region),
			ParentId: fmt.Sprintf("spiffe://%s/spire/server", trustDomain),
			Selectors: []*common.Selector{
				{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("region:%s", region)},
			},
		})

		if createHostgroups {
			// Create a separate host group entry per host with agent UID as the selector
			for _, n := range nodes {
				cluster := selectorValue(n.Selectors, "k8s_psat:cluster")
				uid := selectorValue(n.Selectors, "k8s_psat:agent_node_uid")
				entries = append(entries, &common.RegistrationEntry{
					EntryId:   mustUUID(),
					SpiffeId:  fmt.Sprintf("spiffe://%s/%s", trustDomain, cluster),
					ParentId:  fmt.Sprintf("spiffe://%s/spire/server", trustDomain),
					Selectors: []*common.Selector{{Type: "k8s_psat:agent_node_uid", Value: uid}},
				})
			}
		}
	}

	for region, arches := range archByRegion {
		for arch := range arches {
			entries = append(entries, &common.RegistrationEntry{
				EntryId:  mustUUID(),
				SpiffeId: fmt.Sprintf("spiffe://%s/%s/%s", trustDomain, region, arch),
				ParentId: fmt.Sprintf("spiffe://%s/spire/server", trustDomain),
				Selectors: []*common.Selector{
					{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("region:%s", region)},
					{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("arch:%s", arch)},
				},
			})
		}
	}

	for region, gpus := range gpuByRegion {
		for gpu := range gpus {
			entries = append(entries, &common.RegistrationEntry{
				EntryId:  mustUUID(),
				SpiffeId: fmt.Sprintf("spiffe://%s/%s/%s", trustDomain, region, gpu),
				ParentId: fmt.Sprintf("spiffe://%s/spire/server", trustDomain),
				Selectors: []*common.Selector{
					{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("region:%s", region)},
					{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("gpu:%s", gpu)},
				},
			})
		}
	}

	finish := time.Now().UnixNano()
	delta := (float64(finish) - float64(start)) / 1000000000.0
	fmt.Printf("Created %d entries in %.3f sec (%.3f ops/sec)\n", len(entries), delta, float64(len(entries))/delta)

	if debug {
		for _, e := range entries {
			fmt.Printf("Entry %s, parent %s, selectors %v\n\n", e.SpiffeId, e.ParentId, e.Selectors)
		}
	}

	return entries
}

func storeEntries(clients []*clientv3.Client, all []*common.RegistrationEntry) {
	// Setup the progress bar if requested
	bar = pb.New(len(all))
	if progress {
		bar.Format("==>")
		bar.Start()
	}

	// Store the entries in parallel across the client connections
	start := time.Now().UnixNano()
	totalErrors = 0
	for i := range clients {
		wg.Add(1)
		go func(clientNumber int) {
			defer wg.Done()
			count := len(all) / len(clients)
			c := &etcdClient{c: clients[clientNumber]}
			var entries []*common.RegistrationEntry
			if clientNumber == len(clients)-1 {
				// last task get the remaining items
				entries = all[count*clientNumber:]
			} else {
				entries = all[count*clientNumber : count*(clientNumber+1)]
			}
			for j := 0; j < len(entries); j++ {
				err := c.storeEntry(entries[j], createIndices)
				if !ok(err) {
					return
				}

				if progress {
					bar.Increment()
				}
			}
		}(i)
	}

	wg.Wait()
	finish := time.Now().UnixNano()
	if progress {
		bar.Finish()
	}

	delta := (float64(finish) - float64(start)) / 1000000000.0
	fmt.Printf("%d entries written in %.3f sec (%.3f ops/sec) and %d errors\n", len(all), delta, float64(len(all))/delta, totalErrors)

	return
}

func (ec *etcdClient) storeEntry(e *common.RegistrationEntry, idx bool) error {
	k := store.EntryKey(e.EntryId)
	v, err := proto.Marshal(e)
	if err != nil {
		return err
	}

	ops := []clientv3.Op{}
	ops = append(ops, clientv3.OpPut(k, string(v)))

	// Add index keys if requested
	if idx {
		ops = append(ops, clientv3.OpPut(store.EntryPidKey(e.EntryId, e.ParentId), ""))
		ops = append(ops, clientv3.OpPut(store.EntrySidKey(e.EntryId, e.SpiffeId), ""))

		// NOTE: NodeSelectors works here as SpiffeId and EntryId are both strings
		sel := &datastore.NodeSelectors{SpiffeId: e.EntryId, Selectors: e.Selectors}
		v, err = proto.Marshal(sel)
		if err != nil {
			return err
		}
		ops = append(ops, clientv3.OpPut(store.EntryExpKey(e.EntryId, e.EntryExpiry), string(v)))

		for _, sel := range e.Selectors {
			ops = append(ops, clientv3.OpPut(store.EntrySelKey(e.EntryId, sel), ""))
		}
	}

	// Send the transaction to the etcd cluster
	_, err = ec.c.Txn(context.TODO()).Then(ops...).Commit()
	if err != nil {
		return err
	}

	if debug {
		fmt.Printf("Stored %s\n", k)
	}

	return nil
}

// Create a list of nodes
func createNodes(rc, cc, nc int) []*common.AttestedNode {
	certNotAfter = time.Now().Add(time.Hour * 12).Unix()
	newCertNotAfter = time.Now().Add(time.Hour * 24).Unix()

	nodes := []*common.AttestedNode{}
	start := time.Now().UnixNano()
	for r := 0; r < rc; r++ {
		for c := 0; c < cc; c++ {
			for n := 0; n < nc; n++ {
				node, err := createNode(regions[r], clusters[c], mustUUID(), n)
				if err != nil {
					fmt.Printf("Error generating node %d: %v", n, err)
					os.Exit(1)
				}
				if debug {
					fmt.Printf("Node %s : %v\n\n", node.SpiffeId, node.Selectors)
				}
				nodes = append(nodes, node)
			}
		}
	}
	finish := time.Now().UnixNano()
	delta := (float64(finish) - float64(start)) / 1000000000.0
	fmt.Printf("Created %d nodes in %.3f sec (%.3f ops/sec)\n", len(nodes), delta, float64(len(nodes))/delta)
	return nodes
}

func selectorValue(sel []*common.Selector, key string) string {
	if sel == nil {
		return ""
	}

	for _, s := range sel {
		if s.Type == key {
			return s.Value
		}
	}

	return ""
}

// createNode creates a node.
func createNode(region, cluster, name string, num int) (*common.AttestedNode, error) {
	id := url.URL{
		Scheme: "spiffe",
		Host:   trustDomain,
		Path:   fmt.Sprintf("spire/agent/%s-%s/%s", region, cluster, name),
	}

	// Use parts of the random UUID for other random items to reduce calls
	arch := archValues[name[2]%2]
	gpu := gpuValues[name[3]%2]

	selectors := []*common.Selector{
		{Type: "k8s_psat:cluster", Value: fmt.Sprintf("%s-%s", region, cluster)},
		{Type: "k8s_psat:agent_ns", Value: "spire"},
		{Type: "k8s_psat:agent_sa", Value: "spire-agent"},
		{Type: "k8s_psat:agent_pod_name", Value: fmt.Sprintf("spire-agent-%s", name[0:5])},
		{Type: "k8s_psat:agent_pod_uid", Value: fmt.Sprintf("%s%06d", name[0:30], num)},
		{Type: "k8s_psat:agent_node_ip", Value: fmt.Sprintf("172.16.%d.%d", num/254, num%254+1)},
		{Type: "k8s_psat:agent_node_name", Value: fmt.Sprintf("node%d.%s.%s.%s", num+1, cluster, region, trustDomain)},
		{Type: "k8s_psat:agent_node_uid", Value: name},
		{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("arch:%s", arch)},
		{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("gpu:%s", gpu)},
		{Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("region:%s", region)},
		// {Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("pmem:%s", tf[name[3]%2])},
		// {Type: "k8s_psat:agent_node_label", Value: fmt.Sprintf("bw:%s", bw[name[4]%6])},
	}

	certSerialNumber = fmt.Sprintf("%d%d", rand.Intn(math.MaxInt64), rand.Intn(math.MaxInt64))
	newCertSerialNumber = fmt.Sprintf("%d%d", rand.Intn(math.MaxInt64), rand.Intn(math.MaxInt64))
	randomOffset := int64(rand.Intn(36000))
	n := &common.AttestedNode{
		SpiffeId:            id.String(),
		AttestationDataType: "k8s_psat",
		CertNotAfter:        certNotAfter - randomOffset,
		CertSerialNumber:    certSerialNumber,
		NewCertNotAfter:     newCertNotAfter - randomOffset,
		NewCertSerialNumber: newCertSerialNumber,
		Selectors:           selectors,
	}

	// Add the node to the appropriate entry lists
	nodesByRegion[region] = append(nodesByRegion[region], n)
	archByRegion[region][arch] = append(archByRegion[region][arch], n)
	gpuByRegion[region][gpu] = append(gpuByRegion[region][gpu], n)

	return n, nil
}

func storeNodes(clients []*clientv3.Client, allNodes []*common.AttestedNode, nodesPerClient int) {
	// Setup the progress bar if requested
	bar = pb.New(totalNodes)
	if progress {
		bar.Format("==>")
		bar.Start()
	}

	// Store the nodes in parallel across the client connections
	start := time.Now().UnixNano()
	total := 0
	for i := range clients {
		wg.Add(1)
		go func(clientNumber, count int) {
			defer wg.Done()
			c := &etcdClient{c: clients[clientNumber]}
			nodes := allNodes[count*clientNumber : count*(clientNumber+1)]
			for j := 0; j < count; j++ {
				err := c.storeNode(nodes[j], createIndices)
				if !ok(err) {
					return
				}

				if progress {
					bar.Increment()
				}

				mu.Lock()
				total++
				tn := total
				mu.Unlock()

				if tn >= totalNodes {
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
	fmt.Printf("%d nodes written in %.3f sec (%.3f ops/sec) and %d errors\n", total, delta, float64(total)/delta, totalErrors)

	return
}

func (ec *etcdClient) storeNode(n *common.AttestedNode, idx bool) error {
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

// Return an exponential number of region and cluster names based on total node count
func regionsAndClusters(total int) (r, c int) {
	r = int(math.Log(float64(total)) - 3)
	if r < 1 {
		r = 1
	}

	if total < 10 {
		c = 1
	} else if total < 1000 {
		c = int(math.Log(float64(total)/6) / 0.5)
	} else {
		c = int(math.Log(float64(total)/464)/0.077 + 1)
	}
	return
}
