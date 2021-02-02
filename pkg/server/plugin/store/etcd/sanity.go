// Package etcd implements KV backend Store
package etcd

import (
	"context"
	"fmt"

	"github.com/spiffe/spire/pkg/server/plugin/store"
)

/////////////////////////////
// TODO remove these tests //
/////////////////////////////
func (st *Plugin) sanity() error {
	st.log.Info("Create")
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	_, err := st.Create(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{{Key: "key1", Value: []byte("value1")}},
	})
	cancel()
	if err != nil {
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	_, err = st.Create(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{
			{Key: "key2", Value: []byte("value2")},
			{Key: "key10", Value: []byte("value10")},
			{Key: "key11", Value: []byte("value11")},
		}})
	cancel()
	if err != nil {
		return err
	}

	st.log.Info("Update")
	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	_, err = st.Update(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{{Key: "key1", Value: []byte("value01")}},
	})
	cancel()
	if err != nil {
		return err
	}

	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	_, err = st.Update(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{
			{Key: "key10", Value: []byte("value10a")},
			{Key: "key11", Value: []byte("value11a")},
		}})
	cancel()
	if err != nil {
		return err
	}

	st.log.Info("Get")
	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	res, err := st.Get(ctx, &store.GetRequest{Key: "key1"})
	cancel()
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("%d %d %v", res.Total, res.Revision, res.Kvs)
	st.log.Info(msg)

	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	res, err = st.Get(ctx, &store.GetRequest{Key: "key1", End: "key2"})
	cancel()
	if err != nil {
		return err
	}
	msg = fmt.Sprintf("%d %d %v", res.Total, res.Revision, res.Kvs)
	st.log.Info(msg)

	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	_, err = st.Delete(ctx, &store.DeleteRequest{
		Kvs:   []*store.KeyValue{{Key: "key1"}, {Key: "key2", Version: 0}},
		Range: &store.Range{Key: "key10", End: "key12"},
	})
	cancel()
	if err != nil {
		return err
	}

	return nil
}
