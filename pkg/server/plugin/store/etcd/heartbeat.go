package etcd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	ss "github.com/spiffe/spire/pkg/server/store"

	"github.com/roguesoftware/etcd/clientv3"
)

const (
	// Default heartbeat interval in seconds
	heartbeatDefaultInterval = 5

	// Lifespan of heartbeat records in seconds before automatic deletion
	heartbeatTTL = 1
)

// StartHeartbeatService initializes inter- and intra-server heartbeat monitoring.
// Note: the returned store revision is both the server ID as well as the initial cache version
// Heartbeats are short-lived records PUT in etcd by servers and responded to by
// other servers. Heartbeat keys include originator and responder IDs and the value
// is a timestamp from the originator. In this way, servers can track the latency
// from database write to async watch update across servers and time.
// TODO use heartbeat data to modulate write response time to ensure (improve) inter-server cache coherency.
func (st *Plugin) StartHeartbeatService() (int64, error) {
	// Secure a unique store revision with an empty heartbeat
	ctx := context.TODO()
	rev, err := st.sendHB(ctx, "", "", 1)
	if err != nil {
		return 0, fmt.Errorf("Error getting heartbeat ID %v", err)
	}

	if st.heartbeatInterval == 0 {
		st.log.Warn("Heartbeat disabled")
		return rev, nil
	}

	st.log.Info(fmt.Sprintf("Starting heartbeat with id %d", rev))
	fmt.Printf("Starting heartbeat with id %d\n", rev)
	go st.heartbeatReply(context.TODO(), rev+1)
	go st.heartbeatSend(rev + 1)

	// Handle returns from above

	return rev, nil
}

// Send periodic heartbeat messages
func (st *Plugin) heartbeatSend(rev int64) {
	id := fmt.Sprintf("%d", rev)
	// Loop every interval forever
	ticker := st.clock.Ticker(st.heartbeatInterval)
	for t := range ticker.C {
		st.log.Info(fmt.Sprintf("Sending heartbeat %q at %d", id, t.UnixNano()))
		fmt.Printf("Sending heartbeat %q at %d\n", id, t.UnixNano())
		st.sendHB(context.TODO(), id, "", t.UnixNano())
	}
}

// Reply to heartbeat messages from other servers.
func (st *Plugin) heartbeatReply(ctx context.Context, rev int64) {
	// Watch heartbeat records created after we initialized
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	hc := st.etcd.Watch(context.Background(), ss.HeartbeatPrefix, opts...)

	id := fmt.Sprintf("%d", rev)
	for w := range hc {
		if w.Err() != nil {
			st.log.Error(fmt.Sprintf("Heartbeat channel error %v", w.Err()))
			return
		}

		if w.IsProgressNotify() {
			st.log.Error("No heartbeats for 10 minutes")
		}

		for _, e := range w.Events {
			if e.Type == 1 {
				// Ignore delete operations
				continue
			}
			originator, responder, ts := st.parseHB(e)
			delta := float64(st.clock.Now().UnixNano()-ts) / 1000000.0
			if originator == id {
				if responder == "" {
					st.log.Info(fmt.Sprintf("self heartbeat in %.2fms", delta))
					fmt.Printf("self heartbeat in %.2fms\n", delta)
				} else {
					st.log.Info(fmt.Sprintf("reply heartbeat from %s in %.2fms", responder, delta))
				}
			} else if originator != "" && responder == "" {
				// reply to foreign heartbeat
				st.log.Debug(fmt.Sprintf("reply to %s", originator))
				_, err := st.sendHB(ctx, originator, id, ts)
				if err != nil {
					st.log.Error(fmt.Sprintf("Error sending heartbeat reply to %s %v", originator, err))
				}
			}
		}
	}
}

// Send a heartbeat and return the store revision.
// Heartbeats are formatted as "H|<originator>|<responder>"
func (st *Plugin) sendHB(ctx context.Context, orig, resp string, ts int64) (int64, error) {
	lease, err := st.etcd.Grant(ctx, heartbeatTTL)
	if err != nil {
		st.log.Error("Failed to acquire heartbeat lease")
		return 0, err
	}

	key := fmt.Sprintf("%s%s%s%s", ss.HeartbeatPrefix, orig, ss.Delim, resp)
	value := fmt.Sprintf("%d", ts)
	res, err := st.etcd.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		return 0, err
	}

	return res.Header.Revision, nil
}

// Parse a heartbeat and return the originator and responder ID strings and the timestamp
func (st *Plugin) parseHB(hb *clientv3.Event) (string, string, int64) {
	ts, err := strconv.ParseInt(string(hb.Kv.Value), 10, 64)
	if err != nil {
		st.log.Error(fmt.Sprintf("Invalid heartbeat payload %q %v", string(hb.Kv.Value), hb))
		return "", "", 0
	}

	items := strings.Split(string(hb.Kv.Key), ss.Delim)
	if len(items) == 2 {
		return items[1], "", ts
	}

	if len(items) == 3 {
		return items[1], items[2], ts
	}

	st.log.Error(fmt.Sprintf("Invalid heartbeat %q", string(hb.Kv.Key)))

	return "", "", 0
}
