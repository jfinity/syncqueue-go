package queue

import (
	"sync/atomic"

	"github.com/jfinity/syncqueue-go/mirror"
)

type AtomicEnqueue struct {
	mirror.EnqueueRange
	values []atomic.Value
}

func (ae AtomicEnqueue) Values() []atomic.Value {
	return ae.values
}

type AtomicDequeue struct {
	mirror.DequeueRange
	values []atomic.Value
}

func (ad AtomicDequeue) Values() []atomic.Value {
	return ad.values
}

type AtomicQueue struct {
	manager mirror.QueueManager
	buffer  []atomic.Value
}

type AtomicFactory func() interface{}

func NewAtomic(limit, minPages uint64, factory AtomicFactory) (AtomicQueue, uint64, uint64) {
	qm, extra, needed := mirror.NewManager(limit, minPages)

	aq := AtomicQueue{
		manager: qm,
		buffer:  make([]atomic.Value, qm.Capacity()),
	}

	for index := range aq.buffer {
		aq.buffer[index].Store(factory())
	}

	return aq, extra, needed
}

func (aq AtomicQueue) Enqueue(max uint64) (AtomicEnqueue, uint64) {
	enqr, retry := aq.manager.Enqueue(max)
	begin := enqr.BeginInc()
	finish := enqr.FinishExc()
	return AtomicEnqueue{EnqueueRange: enqr, values: aq.buffer[begin:finish:finish]}, retry
}

func (aq AtomicQueue) Dequeue(max uint64) (AtomicDequeue, uint64) {
	deqr, retry := aq.manager.Dequeue(max)
	begin := deqr.BeginInc()
	finish := deqr.FinishExc()
	return AtomicDequeue{DequeueRange: deqr, values: aq.buffer[begin:finish:finish]}, retry
}
