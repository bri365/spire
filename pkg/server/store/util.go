package store

// removeString removes the given string from an array, if present
func removeString(a []string, s string) []string {
	for i, v := range a {
		if v == s {
			a[i] = a[len(a)-1]
			return a[:len(a)-1]
		}
	}
	return a
}
