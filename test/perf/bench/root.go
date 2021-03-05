package main

import (
	"sync"
	"time"

	"github.com/roguesoftware/etcd/pkg/transport"

	"github.com/spf13/cobra"
	pb "gopkg.in/cheggaaa/pb.v1"
)

var rootCmd = &cobra.Command{
	Use:   "bench",
	Short: "A performance benchmark tool for spire",
	Long:  `bench is a performance support tool for spire`,
}

// KeyRange represents one or more keys as key, (key:end]), or key as a prefix
type KeyRange struct {
	key    string
	end    string
	prefix bool
}

var (
	dialTimeout time.Duration
	endpoints   []string
	tls         transport.TLSInfo

	bar *pb.ProgressBar
	mu  sync.Mutex
	wg  sync.WaitGroup

	all          bool
	debug        bool
	progress     bool
	show         bool
	total        int
	totalConns   uint
	totalClients uint
	totalErrors  uint
	trustDomain  string
)

func init() {
	rootCmd.PersistentFlags().DurationVar(&dialTimeout, "dial-timeout", 0, "dial timeout for client connections")
	rootCmd.PersistentFlags().StringSliceVar(&endpoints, "endpoints", []string{}, "gRPC endpoints")
	rootCmd.PersistentFlags().StringVar(&tls.CertFile, "cert", "", "identify HTTPS client using this SSL certificate file")
	rootCmd.PersistentFlags().StringVar(&tls.KeyFile, "key", "", "identify HTTPS client using this SSL key file")
	rootCmd.PersistentFlags().StringVar(&tls.TrustedCAFile, "cacert", "", "verify certificates of HTTPS-enabled servers using this CA bundle")

	rootCmd.PersistentFlags().UintVar(&totalConns, "conns", 1, "Total number of gRPC connections")
	rootCmd.PersistentFlags().UintVar(&totalClients, "clients", 1, "Total number of gRPC clients")

	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "output debug messages")
}
