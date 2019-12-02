package queue

import (
	"fmt"
	"sync"
)

var ErrQueueIsEmpty = fmt.Errorf("queue is empty")
var ErrNoName = fmt.Errorf("Name is blank for queue")

type Queue struct {
	name  string
	items []interface{}
	mx    sync.Mutex
}

func (q *Queue) String() string {
	return q.name
}

func NewQueue(name string, items []interface{}) (*Queue, error) {
	if name == "" {
		return nil, ErrNoName
	}
	if items == nil {
		items = []interface{}{}
	}

	return &Queue{
		name:  name,
		items: items,
	}, nil
}

func (q *Queue) InsertItem(item interface{}) {
	q.mx.Lock()
	defer q.mx.Unlock()
	q.items = append(q.items, item)
}

func (q *Queue) Pop() (interface{}, error) {
	q.mx.Lock()
	defer q.mx.Unlock()
	if q.isEmpty() {
		return nil, ErrQueueIsEmpty
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, nil
}

func (q *Queue) ListItems() []interface{} {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.items
}

func (q *Queue) IsEmpty() bool {
	q.mx.Lock()
	defer q.mx.Unlock()
	return q.isEmpty()
}

func (q *Queue) isEmpty() bool {
	return len(q.items) == 0
}
