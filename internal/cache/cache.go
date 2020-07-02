package cache

// Cache Cache
type Cache interface{}

type cache struct{}

// New New
func New() Cache {
	return &cache{}
}
