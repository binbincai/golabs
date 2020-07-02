package raft

type electCounter struct {
	term int
	approved map[int]bool
	total int
}

func newElectCounter(term, total int) *electCounter {
	return &electCounter{
		term: term,
		approved: make(map[int]bool),
		total: total,
	}
}

func (e *electCounter) approve(term, server int) bool {
	if e.term == term {
		if _, ok := e.approved[server]; !ok {
			e.approved[server] = true
		}
	}
	return len(e.approved)*2 > e.total
}
