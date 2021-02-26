package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/roguesoftware/etcd/clientv3"
	"github.com/roguesoftware/etcd/mvcc/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Heartbeats are short-lived records in the store PUT and responded to by all servers.
// Heartbeat keys include a unique identifier with originator and responder IDs
// Heartbeat values are timestamps from the originator. With these, servers can track the
// latency from database write to async watch update across all servers over time.
// TODO use heartbeat data to modulate write response delay to improve inter-server cache coherency.

// Store cache constants
const (
	HeartbeatDefaultInterval = 5

	hbTTL = 5
)

var (
	hbCount       int64
	hbAvgRespUsec int64
	hbMinRespUsec int64 = 9999999
	hbMaxRespUsec int64
)

// startHeartbeatService initializes server heartbeat monitoring.
func (s *Shim) startHeartbeatService() (int64, error) {
	// Secure a unique store revision with an empty heartbeat
	ctx := context.TODO()
	rev, err := s.sendHB(ctx, "", "", 1)
	if err != nil {
		return 0, fmt.Errorf("Heartbeat: error getting ID %v", err)
	}

	if s.c.heartbeatInterval == 0 {
		s.Log.Warn("Heartbeat disabled")
		return rev, nil
	}

	s.Log.Info("Heartbeat starting", "id", rev)
	go s.hbReply(context.TODO(), rev+1)
	go s.hbSend(rev + 1)

	return rev, nil
}

// Send periodic heartbeat messages.
// TODO add shutdown protocol
func (s *Shim) hbSend(rev int64) {
	id := fmt.Sprintf("%d", rev)
	// Loop forever, sending heartbeats at configured interval
	ticker := s.clock.Ticker(s.c.heartbeatInterval)
	for t := range ticker.C {
		s.sendHB(context.TODO(), id, "", t.UnixNano())
	}
}

// Reply to heartbeat messages from other servers.
// TODO add shutdown protocol
func (s *Shim) hbReply(ctx context.Context, rev int64) {
	// Watch heartbeat records created after we initialized
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	hc := s.Etcd.Watch(context.Background(), HeartbeatPrefix, opts...)

	id := fmt.Sprintf("%d", rev)
	for w := range hc {
		if w.Err() != nil {
			if status.Convert(w.Err()).Code() != codes.Canceled {
				s.Log.Error("Heartbeat channel error", "error", w.Err())
			}
			return
		}

		if w.IsProgressNotify() {
			s.Log.Error("No heartbeats received for 10 minutes")
		}

		for _, e := range w.Events {
			if e.Type == mvccpb.DELETE {
				continue
			}
			originator, responder, ts := s.parseHB(e)
			deltaUsec := (s.clock.Now().UnixNano() - ts) / 1000
			if originator == id {
				if responder == "" {
					s.Log.Debug(fmt.Sprintf("self heartbeat in %d Usec", deltaUsec))
				} else {
					s.Log.Debug(fmt.Sprintf("reply heartbeat from %s in %d Usec", responder, deltaUsec))
					hbAvgRespUsec = (hbCount*hbAvgRespUsec + deltaUsec*1000) / (hbCount + 1)
					if deltaUsec < hbMinRespUsec {
						hbMinRespUsec = deltaUsec
						s.Log.Info("HB msec", "min", float64(deltaUsec/1000.0))
					}
					if deltaUsec > hbMaxRespUsec {
						hbMaxRespUsec = deltaUsec
						s.Log.Info("HB msec", "max", float64(deltaUsec/1000.0))
					}
					if deltaUsec-hbAvgRespUsec > 10000 {
						// latency more than 10 milliseconds over average
						s.Log.Info("HB latency", "msec", float64(deltaUsec/1000.0), "avg", hbAvgRespUsec)
					}
				}
			} else if originator != "" && responder == "" {
				// reply to foreign heartbeat
				_, err := s.sendHB(ctx, originator, id, ts)
				if err != nil {
					s.Log.Error("Heartbeat error sending reply", "originator", originator, "error", err)
				}
			}
		}
	}
}

// Send a heartbeat and return the store revision.
// Heartbeats are formatted as "H|<originator>|<responder>"
func (s *Shim) sendHB(ctx context.Context, orig, resp string, ts int64) (int64, error) {
	lease, err := s.Etcd.Grant(ctx, hbTTL)
	if err != nil {
		s.Log.Error("Heartbeat failed to acquire lease", "error", err)
		return 0, err
	}

	key := fmt.Sprintf("%s%s%s%s", HeartbeatPrefix, orig, Delim, resp)
	value := fmt.Sprintf("%d", ts)
	res, err := s.Etcd.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		return 0, err
	}

	return res.Header.Revision, nil
}

// Parse a heartbeat and return the originator and responder ID strings and the timestamp
func (s *Shim) parseHB(hb *clientv3.Event) (string, string, int64) {
	ts, err := strconv.ParseInt(string(hb.Kv.Value), 10, 64)
	if err != nil {
		s.Log.Error("Heartbeat invalid payload", "value", string(hb.Kv.Value), "key", hb.Kv.Key)
		return "", "", 0
	}

	items := strings.Split(string(hb.Kv.Key), Delim)
	if len(items) == 2 {
		return items[1], "", ts
	}

	if len(items) == 3 {
		return items[1], items[2], ts
	}

	s.Log.Error("Heartbeat invalid key", "key", string(hb.Kv.Key))

	return "", "", 0
}
