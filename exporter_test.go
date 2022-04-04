package scheduler

import (
	"github.com/googleapis/gax-go/v2"
)

func (s *Scheduler) SetIterator(f func(...gax.CallOption) *Iterator) {
	s.iterator = f
}

func (i *Iterator) SetLister(lister TaskLister) {
	i.lister = lister
}
