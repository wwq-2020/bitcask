package index

// Item Item
type Item struct {
	ID     int64
	Offset int64
}

// Index Index
type Index interface {
	Get(string) (*Item, bool)
	Put(string, *Item)
}

type index struct {
	m map[string]*Item
}

// New New
func New() Index {
	return &index{
		m: make(map[string]*Item),
	}
}

func (i *index) Get(key string) (*Item, bool) {
	item, exist := i.m[key]
	return item, exist
}

func (i *index) Put(key string, item *Item) {
	i.m[key] = item
}
