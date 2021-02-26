package store

import (
	"context"
	"fmt"

	v3 "github.com/roguesoftware/etcd/clientv3"
)

// Get the current store revision.
func (s *Shim) getStoreRevision(ctx context.Context) (int64, error) {
	opts := []v3.OpOption{v3.WithLimit(1), v3.WithRange("}")}
	res, err := s.Etcd.Get(ctx, " ", opts...)
	if err != nil {
		return 0, err
	}

	return res.Header.Revision, nil
}

// removeString removes the given string from an unordered array.
func removeString(a []string, s string) []string {
	for i, v := range a {
		if v == s {
			a[i] = a[len(a)-1]
			return a[:len(a)-1]
		}
	}
	return a
}

// stringPlusOne returns a string with one bit added to the given string.
func stringPlusOne(s string) string {
	return fmt.Sprintf("%s ", s)

}
