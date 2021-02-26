package main

import (
	"bufio"
	"context"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	math_rand "math/rand"
	"os"

	"github.com/roguesoftware/etcd/clientv3"
	"google.golang.org/grpc/grpclog"
)

type etcdClient struct {
	c *clientv3.Client
}

const (
	chars                 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	domainLength          = 3
	hostComponentMaxCount = 5
	hostComponentMaxSize  = 40
	pathComponentMaxCount = 8
	pathComponentMaxSize  = 50
)

var (
	// dialTotal counts the number of mustCreateConn calls so that endpoint
	// connections can be handed out in round-robin order
	dialTotal int

	// leaderEps is a cache for holding endpoints of a leader node
	leaderEps []string

	tds   = []string{}
	words = []string{}
)

func ok(err error) bool {
	if err == nil {
		return true
	}

	mu.Lock()
	totalErrors++
	mu.Unlock()
	return false
}

func mustFindLeaderEndpoints(c *clientv3.Client) {
	resp, lerr := c.MemberList(context.TODO())
	if lerr != nil {
		fmt.Fprintf(os.Stderr, "failed to get a member list: %s\n", lerr)
		os.Exit(1)
	}

	leaderID := uint64(0)
	for _, ep := range c.Endpoints() {
		if sresp, serr := c.Status(context.TODO(), ep); serr == nil {
			leaderID = sresp.Leader
			break
		}
	}

	for _, m := range resp.Members {
		if m.ID == leaderID {
			leaderEps = m.ClientURLs
			return
		}
	}

	fmt.Fprintf(os.Stderr, "failed to find a leader endpoint\n")
	os.Exit(1)
}

func mustCreateConn() *clientv3.Client {
	connEndpoints := leaderEps
	if len(connEndpoints) == 0 {
		connEndpoints = []string{endpoints[dialTotal%len(endpoints)]}
		dialTotal++
	}
	cfg := clientv3.Config{
		Endpoints:   connEndpoints,
		DialTimeout: dialTimeout,
	}
	if !tls.Empty() || tls.TrustedCAFile != "" {
		cfgtls, err := tls.ClientConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad tls config: %v\n", err)
			os.Exit(1)
		}
		cfg.TLS = cfgtls
	}

	client, err := clientv3.New(cfg)
	clientv3.SetLogger(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))

	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}

	return client
}

func mustCreateClients(totalClients, totalConns uint) []*clientv3.Client {
	conns := make([]*clientv3.Client, totalConns)
	for i := range conns {
		conns[i] = mustCreateConn()
	}

	clients := make([]*clientv3.Client, totalClients)
	for i := range clients {
		clients[i] = conns[i%int(totalConns)]
	}
	return clients
}

func mustRandBytes(n int) []byte {
	rb := make([]byte, n)
	_, err := crypto_rand.Read(rb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate value: %v\n", err)
		os.Exit(1)
	}
	return rb
}

func randInit() {
	// Seed math/rand for improved randomness
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand with crypto/rand")
	}
	math_rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))

	// Get domain names
	f, err := os.Open("tds.txt")
	if err != nil {
		panic("Error opening domain file")
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		tds = append(tds, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		panic("Error reading domain file")
	}

	// Get common words for host names and paths
	f, err = os.Open("words.txt")
	if err != nil {
		panic("Error opening word file")
	}

	scanner = bufio.NewScanner(f)
	for scanner.Scan() {
		words = append(words, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		panic("Error reading word file")
	}
}

func randString(length int) string {
	s := ""
	for i := 0; i < length; i++ {
		s = fmt.Sprintf("%s%c", s, chars[rand.Intn(len(chars))])
	}
	return s
}

func randPath(pathPartCount int) string {
	path := ""
	if pathPartCount > 0 && pathPartCount <= pathComponentMaxCount {
		path = fmt.Sprintf("/%s", words[math_rand.Intn(len(words))])
		for i := 1; i < pathPartCount; i++ {
			path = fmt.Sprintf("%s/%s", path, words[math_rand.Intn(len(words))])
		}
	}

	return path
}

func getThreeWordLists(n int) (a, b, c []string) {
	a = []string{}
	b = []string{}
	c = []string{}
	if n < 1 {
		return
	}

	// get a set of unique words
	wordSet := map[string]struct{}{}
	for {
		wordSet[words[math_rand.Intn(len(words))]] = struct{}{}
		if len(wordSet) > 3*n {
			break
		}
	}

	wordList := []string{}
	for k := range wordSet {
		wordList = append(wordList, k)
	}

	a = wordList[0:n]
	b = wordList[n : n*2]
	c = wordList[n*2 : n*3]

	return
}
