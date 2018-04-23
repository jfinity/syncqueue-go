package mirror

import (
	"math/bits"
	"sync/atomic"
)

type naiveCacheLinePadding [64]byte

func calculateCycle(offset, bitshift uint64) uint64 {
	return offset >> bitshift
}

type QueueAtom struct {
	consumed  uint64
	expected  uint64
	expecting uint64

	produced  uint64
	reserving uint64

	targeted  uint64
	targeting uint64
	reserved  uint64
}

func (qa *QueueAtom) Expect(amount uint64, cycle uint64) bool {
	if atomic.CompareAndSwapUint64(&qa.consumed, (cycle<<1)+0, (cycle<<1)+3) {
		atomic.StoreUint64(&qa.expecting, amount)
		atomic.StoreUint64(&qa.expected, (cycle<<1)+1)
		return true
	}
	return false
}

func (qa *QueueAtom) Expecting() uint64 {
	return atomic.LoadUint64(&qa.expecting)
}

func (qa *QueueAtom) Produce(cycle uint64) bool {
	if atomic.CompareAndSwapUint64(&qa.expected, (cycle<<1)+1, (cycle<<1)+2) {
		atomic.StoreUint64(&qa.produced, (cycle<<1)+1)
		return true
	}
	return false
}

func (qa *QueueAtom) Untargetable(cycle uint64) bool {
	return atomic.LoadUint64(&qa.produced) != (cycle<<1)+1
}

func (qa *QueueAtom) Target(amount uint64, cycle uint64) bool {
	if atomic.CompareAndSwapUint64(&qa.produced, (cycle<<1)+1, (cycle<<1)+2) {
		atomic.StoreUint64(&qa.targeting, amount)
		atomic.StoreUint64(&qa.targeted, (cycle<<1)+1)
		return true
	}
	return false
}

func (qa *QueueAtom) Targeting() uint64 {
	return atomic.LoadUint64(&qa.targeting)
}

func (qa *QueueAtom) Reserve(amount uint64, cycle uint64) bool {
	if atomic.CompareAndSwapUint64(&qa.targeted, (cycle<<1)+1, (cycle<<1)+2) {
		atomic.StoreUint64(&qa.reserving, amount)
		atomic.StoreUint64(&qa.targeting, amount)
		atomic.StoreUint64(&qa.reserved, (cycle<<1)+1)
		return true
	}
	return false
}

func (qa *QueueAtom) Reserving() uint64 {
	return atomic.LoadUint64(&qa.reserving)
}

func (qa *QueueAtom) Consume(cycle uint64) bool {
	if atomic.CompareAndSwapUint64(&qa.reserved, (cycle<<1)+1, (cycle<<1)+2) {
		atomic.StoreUint64(&qa.reserving, 0)
		atomic.StoreUint64(&qa.targeting, 0)
		atomic.StoreUint64(&qa.expecting, 0)
		atomic.StoreUint64(&qa.consumed, (cycle<<1)+2)
		return true
	}
	return false
}

type AtomRange struct {
	atoms    []QueueAtom
	size     uint64
	offset   uint64
	bitshift uint64
}

func (ar AtomRange) Size() uint64 {
	return ar.size
}

func (ar AtomRange) BeginInc() uint64 {
	return (ar.offset & ((1 << ar.bitshift) - 1))
}

func (ar AtomRange) FinishExc() uint64 {
	return ar.BeginInc() + ar.Size()
}

type EnqueueRange struct {
	AtomRange
}

func (enqr EnqueueRange) Produce() {
	amount := uint64(len(enqr.atoms))
	cycle := calculateCycle(enqr.offset, enqr.bitshift)

	for index := amount; index > 0; index-- {
		enqr.atoms[index-1].Produce(cycle)
	}
}

type DequeueRange struct {
	AtomRange
	total *uint64
}

func (deqr DequeueRange) Consume() bool {
	amount := uint64(len(deqr.atoms))
	cycle := calculateCycle(deqr.offset, deqr.bitshift)
	total := uint64(0)

	if deqr.total != nil {
		for index := amount; index > 0; index-- {
			if deqr.atoms[index-1].Consume(cycle) {
				total++
			}
		}

		atomic.AddUint64(deqr.total, total)
		return true
	}
	return false
}

type queuePage struct {
	enqueueOffset     uint64
	_                 naiveCacheLinePadding
	dequeueOffsetHint uint64
	_                 naiveCacheLinePadding
	recycleTotal      uint64
	_                 naiveCacheLinePadding
}

type queueCounters struct {
	readPage   uint64
	_          naiveCacheLinePadding
	writePage  uint64
	_          naiveCacheLinePadding
	deletePage uint64
	_          naiveCacheLinePadding
}

type QueueManager struct {
	atoms    []QueueAtom
	pages    []queuePage
	counters *queueCounters //TODO: should this be a slice (for copying/escape-analysis reasons)?
	atomMask uint64
	pageMask uint64
	bitshift uint64
}

func NewManager(limit uint64, minPages uint64) (qm QueueManager, extra uint64, needed uint64) {
	pageShift := uint64(bits.Len64(minPages))

	if pageShift < 1 {
		pageShift = 1
	} else if minPages == 1<<(pageShift-1) {
		pageShift--
	}

	pageAmount := uint(1 << pageShift)
	pageMask := uint64(pageAmount - 1)

	maxAtoms := uint(limit) / pageAmount
	atomShift := uint64(bits.Len(maxAtoms)) - 1
	atomAmount := uint(1 << atomShift)
	atomMask := uint64(atomAmount - 1)

	count := uint64(pageAmount) * uint64(atomAmount)

	qm = QueueManager{
		atoms:    make([]QueueAtom, count),
		pages:    make([]queuePage, pageAmount),
		counters: &queueCounters{},
		pageMask: pageMask,
		atomMask: atomMask,
		bitshift: atomShift + pageShift,
	}

	extra = limit - count

	needed = extra

	if needed > 0 {
		needed = uint64(pageAmount) - needed
	}

	return
}

func (qm QueueManager) Capacity() uint64 {
	return (1 + qm.atomMask) * (1 + qm.pageMask)
}

func (qm QueueManager) readPage() uint64 {
	return atomic.LoadUint64(&qm.counters.readPage)
}

func (qm QueueManager) writePage() uint64 {
	return atomic.LoadUint64(&qm.counters.writePage)
}

func (qm QueueManager) deletePage() uint64 {
	return atomic.LoadUint64(&qm.counters.deletePage)
}

func (qm QueueManager) turnReadPage() uint64 {
	return atomic.AddUint64(&qm.counters.readPage, 1)
}

func (qm QueueManager) turnWritePage() uint64 {
	return atomic.AddUint64(&qm.counters.writePage, 1)
}

func (qm QueueManager) turnDeletePage() uint64 {
	return atomic.AddUint64(&qm.counters.deletePage, 1)
}

func (qm QueueManager) finishedRecycling(pageIndex uint64) bool {
	return atomic.CompareAndSwapUint64(&qm.pages[pageIndex].recycleTotal, 1+qm.atomMask, 0)
}

func (qm QueueManager) advanceEnqueueOffset(pageIndex, amount uint64) uint64 {
	return atomic.AddUint64(&qm.pages[pageIndex].enqueueOffset, uint64(amount))
}

func (qm QueueManager) getDequeueOffsetHint(pageIndex uint64) uint64 {
	return atomic.LoadUint64(&qm.pages[pageIndex].dequeueOffsetHint)
}

func (qm QueueManager) proposeDequeueOffsetHint(pageIndex, hint uint64) {
	old := qm.getDequeueOffsetHint(pageIndex)
	if old < hint {
		atomic.CompareAndSwapUint64(&qm.pages[pageIndex].dequeueOffsetHint, old, hint)
	}
}

func (qm QueueManager) resetPageCounters(pageIndex uint64) {
	atomic.StoreUint64(&qm.pages[pageIndex].enqueueOffset, 0)
	atomic.StoreUint64(&qm.pages[pageIndex].dequeueOffsetHint, 0)
	atomic.StoreUint64(&qm.pages[pageIndex].recycleTotal, 0)
}

func (qm QueueManager) Enqueue(maxCount uint64) (enqr EnqueueRange, retry uint64) {
	retry = maxCount

	deletePage := qm.deletePage()

	for loop := 1 + qm.pageMask; loop > 0; loop-- {
		if qm.finishedRecycling(deletePage) {
			qm.resetPageCounters(deletePage)
			deletePage = qm.turnDeletePage()
		} else {
			break
		}
	}

	writePage := qm.writePage()

	if writePage > deletePage+qm.pageMask {
		return
	}

	pageIndex := writePage & qm.pageMask
	end := qm.advanceEnqueueOffset(pageIndex, uint64(maxCount))
	start := end - uint64(maxCount)

	if start > qm.atomMask {
		return
	} else if end > qm.atomMask {
		qm.turnWritePage()
		retry = end - qm.atomMask - 1
		end = qm.atomMask + 1
	}

	begin := pageIndex*(1+qm.atomMask) + start
	finish := pageIndex*(1+qm.atomMask) + end
	offset := writePage*(1+qm.atomMask) + start
	cycle := calculateCycle(offset, qm.bitshift)

	for index := begin; index < finish; index++ {
		qm.atoms[index].Expect(finish-index, cycle)
	}

	enqr.size = end - start
	enqr.offset = offset
	enqr.bitshift = qm.bitshift
	enqr.atoms = qm.atoms[begin:finish:finish]

	return
}

func (qm QueueManager) Dequeue(maxCount uint64) (deqr DequeueRange, retry uint64) {
	retry = maxCount

	readPage := qm.readPage()

	pageIndex := readPage & qm.pageMask
	pageOffset := qm.getDequeueOffsetHint(pageIndex)
	atomOffset := pageIndex*(1+qm.atomMask) + pageOffset
	offset := readPage*(1+qm.atomMask) + pageOffset
	cycle := calculateCycle(offset, qm.bitshift)

	targeted := false
	targeting := maxCount

	for {
		if atomOffset > qm.atomMask {
			return
		}

		if qm.atoms[atomOffset].Target(maxCount, cycle) {
			targeted = true
			break
		} else {
			skip := qm.atoms[atomOffset].Reserving()
			atomOffset += skip
			pageOffset += skip

			if skip < 1 {
				break
			}
		}
	}

	if !targeted {
		targeting = qm.atoms[atomOffset].Targeting()
	}

	for index := uint64(1); index < targeting; index++ {
		if index+atomOffset > qm.atomMask ||
			(!qm.atoms[index+atomOffset].Target(0, cycle) &&
				qm.atoms[index+atomOffset].Untargetable(cycle)) {
			if qm.atoms[atomOffset].Reserve(index, cycle) {
				qm.proposeDequeueOffsetHint(pageIndex, index+pageOffset)
			}
			break
		}
	}

	if !targeted {
		return
	}

	reserving := qm.atoms[atomOffset].Reserving()

	if reserving+atomOffset > qm.atomMask {
		qm.turnReadPage()
	}

	for index := uint64(1); index < reserving; index++ {
		qm.atoms[index+atomOffset].Reserve(reserving-index, cycle)
	}

	retry = maxCount - reserving

	deqr.size = reserving
	deqr.offset = readPage*(1+qm.atomMask) + pageOffset
	deqr.bitshift = qm.bitshift
	deqr.atoms = qm.atoms[atomOffset : atomOffset+reserving : atomOffset+reserving]
	deqr.total = &qm.pages[pageIndex].recycleTotal

	return
}
