package queue

import (
	"errors"
	"github.com/ozgurrahmidonmez/taskpool/model"
	"sync"
)

type Queue struct {
	mu sync.Mutex
	capacity int
	q  []model.Data
}

type FifoQueue interface {
	Push(item model.Data) error
	PushFront(item model.Data) error
	Pull() (model.Data, error)
	Size() int
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.q)
}

// Push inserts the item into the queue
func (q *Queue) Push(item model.Data) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.q) < q.capacity {
		q.q = append(q.q, item)
		return nil
	}
	return errors.New("queue is full")
}

// PushFront inserts the item into the queue - as first item to be polled
func (q *Queue) PushFront(item model.Data) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.q) < q.capacity {
		q.q = append([]model.Data{item}, q.q)
		return nil
	}
	return errors.New("queue is full")
}

// Pull removes the oldest element from the queue
func (q *Queue) Pull() (model.Data, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.q) > 0 {
		item := q.q[0]
		q.q = q.q[1:]
		return item, nil
	}
return 0, errors.New("queue is empty")
}

// NewQueue creates an empty queue with desired capacity
func NewQueue(capacity int) *Queue {
	return &Queue{capacity: capacity, q: make([]model.Data, 0, capacity),}
}