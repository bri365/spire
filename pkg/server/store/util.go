package store

import "fmt"

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
	if len(s) == 0 {
		return s
	}
	last := s[len(s)-1]
	if last == 0xff {
		return fmt.Sprintf("%s%c", s, 0)
	}
	return fmt.Sprintf("%s%c", s[:len(s)-1], last+1)

}
