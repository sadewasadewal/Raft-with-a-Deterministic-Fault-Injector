package raft

// Peer is one Raft replica. The simulator owns time and delivery.
// Rules follow Ongaro's Raft paper, Figure 2, without membership changes,
// snapshots, or leases.

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Entry struct {
	Term  int    `json:"term"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Message struct {
	From          int
	To            int
	Term          int
	Type          string
	LastLogIndex  int
	LastLogTerm   int
	Granted       bool
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []Entry
	LeaderCommit  int
	Success       bool
	MatchHint     int
	RejectHint    int
}

// Persist is the state that must survive a crash.
type Persist struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry
}

type Peer struct {
	ID          int
	N           int
	CurrentTerm int
	VotedFor    int
	Log         []Entry
	CommitIndex int
	LastApplied int
	Role        Role
	NextIndex   []int
	MatchIndex  []int
	Votes       map[int]bool
	Alive       bool
	Leader      int
}

func New(id, n int) *Peer {
	return &Peer{
		ID:       id,
		N:        n,
		VotedFor: -1,
		Log:      []Entry{{Term: 0}},
		Role:     Follower,
		Alive:    true,
		Leader:   -1,
		Votes:    map[int]bool{},
	}
}

func (p *Peer) LastIndex() int { return len(p.Log) - 1 }

func (p *Peer) LastTerm() int { return p.Log[p.LastIndex()].Term }

func (p *Peer) Snapshot() Persist {
	cp := make([]Entry, len(p.Log))
	copy(cp, p.Log)
	return Persist{CurrentTerm: p.CurrentTerm, VotedFor: p.VotedFor, Log: cp}
}

// Restore reloads persistent state and clears volatile leader state.
func (p *Peer) Restore(s Persist) {
	p.CurrentTerm = s.CurrentTerm
	p.VotedFor = s.VotedFor
	p.Log = append([]Entry(nil), s.Log...)
	if len(p.Log) == 0 {
		p.Log = []Entry{{Term: 0}}
	}
	p.CommitIndex = 0
	p.LastApplied = 0
	p.Role = Follower
	p.Votes = map[int]bool{}
	p.Leader = -1
	p.NextIndex = nil
	p.MatchIndex = nil
	p.Alive = true
}

func (p *Peer) becomeFollower(term int) {
	p.CurrentTerm = term
	p.Role = Follower
	p.VotedFor = -1
	p.Votes = map[int]bool{}
	p.Leader = -1
}

func (p *Peer) logUpToDate(idx, term int) bool {
	myTerm := p.LastTerm()
	if term != myTerm {
		return term > myTerm
	}
	return idx >= p.LastIndex()
}

// OnElectionTimeout starts an election. Caller reschedules the timer.
func (p *Peer) OnElectionTimeout() []Message {
	if !p.Alive || p.Role == Leader {
		return nil
	}
	p.CurrentTerm++
	p.Role = Candidate
	p.VotedFor = p.ID
	p.Votes = map[int]bool{p.ID: true}
	p.Leader = -1
	var out []Message
	for to := 0; to < p.N; to++ {
		if to == p.ID {
			continue
		}
		out = append(out, Message{
			From:         p.ID,
			To:           to,
			Term:         p.CurrentTerm,
			Type:         "RequestVote",
			LastLogIndex: p.LastIndex(),
			LastLogTerm:  p.LastTerm(),
		})
	}
	return out
}

func (p *Peer) becomeLeader() {
	p.Role = Leader
	p.Leader = p.ID
	p.NextIndex = make([]int, p.N)
	p.MatchIndex = make([]int, p.N)
	last := p.LastIndex()
	for i := 0; i < p.N; i++ {
		p.NextIndex[i] = last + 1
		p.MatchIndex[i] = 0
	}
	p.MatchIndex[p.ID] = last
}

// Replicate sends AppendEntries (heartbeats if the suffix is empty).
func (p *Peer) Replicate() []Message {
	if !p.Alive || p.Role != Leader {
		return nil
	}
	var out []Message
	for to := 0; to < p.N; to++ {
		if to == p.ID {
			continue
		}
		next := p.NextIndex[to]
		if next < 1 {
			next = 1
		}
		if next > p.LastIndex()+1 {
			next = p.LastIndex() + 1
		}
		prev := next - 1
		ents := append([]Entry(nil), p.Log[next:]...)
		out = append(out, Message{
			From:         p.ID,
			To:           to,
			Term:         p.CurrentTerm,
			Type:        "AppendEntries",
			PrevLogIndex: prev,
			PrevLogTerm:  p.Log[prev].Term,
			Entries:      ents,
			LeaderCommit: p.CommitIndex,
		})
	}
	return out
}

// Propose appends a command. Only a live leader accepts it.
func (p *Peer) Propose(key, value string) bool {
	if !p.Alive || p.Role != Leader {
		return false
	}
	p.Log = append(p.Log, Entry{Term: p.CurrentTerm, Key: key, Value: value})
	p.MatchIndex[p.ID] = p.LastIndex()
	return true
}

// Step handles one delivered RPC. The bool is true if this peer just became leader.
func (p *Peer) Step(m Message) ([]Message, bool) {
	if !p.Alive {
		return nil, false
	}
	if m.Term > p.CurrentTerm {
		p.becomeFollower(m.Term)
	}
	switch m.Type {
	case "RequestVote":
		return p.onRequestVote(m), false
	case "RequestVoteResp":
		return nil, p.onRequestVoteResp(m)
	case "AppendEntries":
		return p.onAppendEntries(m), false
	case "AppendEntriesResp":
		p.onAppendEntriesResp(m)
		return nil, false
	default:
		return nil, false
	}
}

func (p *Peer) onRequestVote(m Message) []Message {
	granted := false
	if m.Term < p.CurrentTerm {
		granted = false
	} else if (p.VotedFor == -1 || p.VotedFor == m.From) && p.logUpToDate(m.LastLogIndex, m.LastLogTerm) {
		granted = true
		p.VotedFor = m.From
		p.Role = Follower
	}
	return []Message{{
		From:    p.ID,
		To:      m.From,
		Term:    p.CurrentTerm,
		Type:    "RequestVoteResp",
		Granted: granted,
	}}
}

func (p *Peer) onRequestVoteResp(m Message) bool {
	if p.Role != Candidate || m.Term != p.CurrentTerm || !m.Granted {
		return false
	}
	p.Votes[m.From] = true
	if len(p.Votes) > p.N/2 {
		p.becomeLeader()
		return true
	}
	return false
}

func (p *Peer) onAppendEntries(m Message) []Message {
	reply := Message{From: p.ID, To: m.From, Term: p.CurrentTerm, Type: "AppendEntriesResp"}
	if m.Term < p.CurrentTerm {
		reply.Success = false
		reply.RejectHint = p.LastIndex()
		return []Message{reply}
	}
	// Current leader for this term.
	p.Role = Follower
	p.Leader = m.From
	p.Votes = map[int]bool{}
	if m.PrevLogIndex > p.LastIndex() || p.Log[m.PrevLogIndex].Term != m.PrevLogTerm {
		reply.Success = false
		reply.RejectHint = p.LastIndex()
		return []Message{reply}
	}
	// Matching prefix. Drop the conflicting suffix, then append.
	p.Log = p.Log[:m.PrevLogIndex+1]
	p.Log = append(p.Log, m.Entries...)
	if m.LeaderCommit > p.CommitIndex {
		p.CommitIndex = m.LeaderCommit
		if p.CommitIndex > p.LastIndex() {
			p.CommitIndex = p.LastIndex()
		}
	}
	reply.Success = true
	reply.MatchHint = p.LastIndex()
	return []Message{reply}
}

func (p *Peer) onAppendEntriesResp(m Message) {
	if p.Role != Leader || m.Term != p.CurrentTerm {
		return
	}
	if m.Success {
		if m.MatchHint > p.MatchIndex[m.From] {
			p.MatchIndex[m.From] = m.MatchHint
		}
		p.NextIndex[m.From] = p.MatchIndex[m.From] + 1
		p.maybeCommit()
		return
	}
	hint := m.RejectHint
	if hint < 1 {
		hint = 1
	}
	if hint >= p.NextIndex[m.From] && p.NextIndex[m.From] > 1 {
		p.NextIndex[m.From]--
	} else if hint < p.NextIndex[m.From] {
		p.NextIndex[m.From] = hint
	}
	if p.NextIndex[m.From] < 1 {
		p.NextIndex[m.From] = 1
	}
}

func (p *Peer) maybeCommit() {
	for n := p.LastIndex(); n > p.CommitIndex; n-- {
		if p.Log[n].Term != p.CurrentTerm {
			continue
		}
		count := 1
		for id := 0; id < p.N; id++ {
			if id != p.ID && p.MatchIndex[id] >= n {
				count++
			}
		}
		if count > p.N/2 {
			p.CommitIndex = n
			return
		}
	}
}

// Apply pushes newly committed entries into the state machine, in order.
func (p *Peer) Apply(put func(key, value string)) int {
	n := 0
	for p.LastApplied < p.CommitIndex {
		p.LastApplied++
		e := p.Log[p.LastApplied]
		put(e.Key, e.Value)
		n++
	}
	return n
}
