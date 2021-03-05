package main

import (
	"bufio"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	math_rand "math/rand"
	"os"

	"github.com/gofrs/uuid"
	"github.com/roguesoftware/etcd/clientv3"
	"google.golang.org/grpc/grpclog"
)

type etcdClient struct {
	c *clientv3.Client
}

const (
	chars    = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	hex      = "0123456789abcdef"
	lowerNum = "0123456789abcdefghijklmnopqrstuvwxyz"
	numbers  = "0123456789"
)

var (
	// dialTotal counts the number of mustCreateConn calls so that endpoint
	// connections can be handed out in round-robin order
	dialTotal int

	// leaderEps is a cache for holding endpoints of a leader node
	leaderEps []string
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

// Seed math/rand for improved randomness
func initRand() {
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand with crypto/rand")
	}
	math_rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))
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

func mustLoad(name string) []string {
	f, err := os.Open(name)
	if err != nil {
		fmt.Printf("Error opening file %q\n", name)
		os.Exit(1)
	}

	lines := []string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file %q\n", name)
		os.Exit(1)
	}

	return lines
}

func mustRandomFileStrings(name string, count int) []string {
	strings := mustLoad(name)
	if len(strings) < count {
		fmt.Printf("Insufficient strings %d < %d\n", len(strings), count)
		os.Exit(1)
	}

	// get a set of unique strings from file results
	// NOTE: as requested count approaches file size, efficiency decreases
	stringSet := map[string]struct{}{}
	for {
		stringSet[strings[math_rand.Intn(len(strings))]] = struct{}{}
		if len(stringSet) == count {
			break
		}
	}

	stringList := []string{}
	for k := range stringSet {
		stringList = append(stringList, k)
	}

	return stringList
}

func mustUUID() string {
	u, err := uuid.NewV4()
	if err != nil {
		fmt.Printf("Error generating UUID: %v", err)
		os.Exit(1)
	}
	return u.String()
}

func randString(chars string, length int) string {
	s := ""
	for i := 0; i < length; i++ {
		s = fmt.Sprintf("%s%c", s, chars[rand.Intn(len(chars))])
	}
	return s
}
