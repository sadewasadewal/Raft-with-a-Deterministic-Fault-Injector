package sim

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"raftlab/internal/machine"
	"raftlab/internal/raft"
)

// Event is one row in the timeline trace.
type Event struct {
	T      int    `json:"t"`
	Kind   string `json:"kind"`
	Peer   int    `json:"peer"`
	From   int    `json:"from,omitempty"`
	To     int    `json:"to,omitempty"`
	Term   int    `json:"term,omitempty"`
	Role   string `json:"role,omitempty"`
	Commit int    `json:"commit,omitempty"`
	Log    int    `json:"log,omitempty"`
	RPC    string `json:"rpc,omitempty"`
	OK     *bool  `json:"ok,omitempty"`
	Note   string `json:"note,omitempty"`
}

// Trace is what the UI loads.
type Trace struct {
	Seed     int64   `json:"seed"`
	Peers    int     `json:"peers"`
	Scenario string  `json:"scenario"`
	Events   []Event `json:"events"`
}

type timed struct {
	at, seq int
	kind    string
	peer    int
	gen     int
	msg     raft.Message
}

type queue []timed

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)         { *q = append(*q, x.(timed)) }
func (q *queue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[:n-1]
	return item
}

// Sim is a single-process, seeded Raft cluster. Virtual time only.
type Sim struct {
	Seed     int64
	Scenario string
	N        int
	Now      int
	rng      *rand.Rand
	peers    []*raft.Peer
	sms      []*machine.KV
	disk     []raft.Persist
	q        queue
	seq      int
	elecGen  []int
	hbGen    []int
	block    map[[2]int]bool
	Events   []Event
	stopped  bool
}

func New(seed int64, n int, scenario string) *Sim {
	s := &Sim{
		Seed:     seed,
		Scenario: scenario,
		N:        n,
		rng:      rand.New(rand.NewSource(seed)),
		peers:    make([]*raft.Peer, n),
		sms:      make([]*machine.KV, n),
		disk:     make([]raft.Persist, n),
		elecGen:  make([]int, n),
		hbGen:    make([]int, n),
		block:    map[[2]int]bool{},
	}
	heap.Init(&s.q)
	for i := 0; i < n; i++ {
		s.peers[i] = raft.New(i, n)
		s.sms[i] = machine.New()
		s.disk[i] = s.peers[i].Snapshot()
		s.note(i, "boot", "follower")
		s.armElection(i)
	}
	return s
}

func (s *Sim) push(at int, kind string, peer, gen int, msg raft.Message) {
	s.seq++
	heap.Push(&s.q, timed{at: at, seq: s.seq, kind: kind, peer: peer, gen: gen, msg: msg})
}

func (s *Sim) armElection(id int) {
	s.elecGen[id]++
	timeout := 150 + s.rng.Intn(150)
	s.push(s.Now+timeout, "election", id, s.elecGen[id], raft.Message{})
}

func (s *Sim) armHeartbeat(id int) {
	s.hbGen[id]++
	s.push(s.Now+40, "heartbeat", id, s.hbGen[id], raft.Message{})
}

func (s *Sim) note(peer int, kind, role string) {
	p := s.peers[peer]
	s.Events = append(s.Events, Event{
		T: s.Now, Kind: kind, Peer: peer, Term: p.CurrentTerm, Role: role,
		Commit: p.CommitIndex, Log: p.LastIndex(),
	})
}

func (s *Sim) canSend(from, to int) bool {
	if !s.peers[from].Alive || !s.peers[to].Alive {
		return false
	}
	return !s.block[[2]int{from, to}]
}

func (s *Sim) send(m raft.Message) {
	if !s.canSend(m.From, m.To) {
		ok := false
		s.Events = append(s.Events, Event{
			T: s.Now, Kind: "drop", Peer: m.From, From: m.From, To: m.To,
			Term: m.Term, RPC: m.Type, OK: &ok, Note: "blocked or dead",
		})
		return
	}
	delay := 1 + s.rng.Intn(8)
	s.push(s.Now+delay, "deliver", m.To, 0, m)
	s.Events = append(s.Events, Event{
		T: s.Now, Kind: "send", Peer: m.From, From: m.From, To: m.To,
		Term: m.Term, RPC: m.Type, Commit: m.LeaderCommit, Log: len(m.Entries),
	})
}

func (s *Sim) persist(id int) {
	s.disk[id] = s.peers[id].Snapshot()
}

func (s *Sim) after(id int, becameLeader bool) {
	s.persist(id)
	p := s.peers[id]
	p.Apply(func(k, v string) { s.sms[id].Apply(k, v) })
	if becameLeader {
		s.note(id, "role", string(p.Role))
		s.armHeartbeat(id)
		for _, m := range p.Replicate() {
			s.send(m)
		}
	}
}

// Run advances virtual time by ms, processing every due event.
func (s *Sim) Run(ms int) {
	limit := s.Now + ms
	for s.q.Len() > 0 {
		ev := s.q[0]
		if ev.at > limit {
			break
		}
		heap.Pop(&s.q)
		s.Now = ev.at
		switch ev.kind {
		case "election":
			if ev.gen != s.elecGen[ev.peer] || !s.peers[ev.peer].Alive {
				continue
			}
			msgs := s.peers[ev.peer].OnElectionTimeout()
			s.note(ev.peer, "role", string(s.peers[ev.peer].Role))
			s.persist(ev.peer)
			s.armElection(ev.peer)
			for _, m := range msgs {
				s.send(m)
			}
		case "heartbeat":
			if ev.gen != s.hbGen[ev.peer] || !s.peers[ev.peer].Alive || s.peers[ev.peer].Role != raft.Leader {
				continue
			}
			for _, m := range s.peers[ev.peer].Replicate() {
				s.send(m)
			}
			s.maybeCommitNote(ev.peer)
			s.armHeartbeat(ev.peer)
		case "deliver":
			if !s.peers[ev.peer].Alive {
				continue
			}
			before := s.peers[ev.peer].Role
			termBefore := s.peers[ev.peer].CurrentTerm
			resps, led := s.peers[ev.peer].Step(ev.msg)
			ok := true
			if ev.msg.Type == "RequestVoteResp" {
				ok = ev.msg.Granted
			}
			if ev.msg.Type == "AppendEntriesResp" {
				ok = ev.msg.Success
			}
			s.Events = append(s.Events, Event{
				T: s.Now, Kind: "recv", Peer: ev.peer, From: ev.msg.From, To: ev.msg.To,
				Term: ev.msg.Term, RPC: ev.msg.Type, OK: &ok,
			})
			if s.peers[ev.peer].Role != before || s.peers[ev.peer].CurrentTerm != termBefore {
				s.note(ev.peer, "role", string(s.peers[ev.peer].Role))
			}
			// A vote grant resets the election timer (paper: reset on grant).
			if ev.msg.Type == "RequestVote" && len(resps) == 1 && resps[0].Granted {
				s.armElection(ev.peer)
			}
			if ev.msg.Type == "AppendEntries" && len(resps) == 1 && resps[0].Success {
				s.armElection(ev.peer)
			}
			s.after(ev.peer, led)
			for _, m := range resps {
				s.send(m)
			}
			s.maybeCommitNote(ev.peer)
		}
	}
	s.Now = limit
}

func (s *Sim) maybeCommitNote(id int) {
	p := s.peers[id]
	if p.CommitIndex > 0 && p.LastApplied == p.CommitIndex {
		s.Events = append(s.Events, Event{
			T: s.Now, Kind: "commit", Peer: id, Term: p.CurrentTerm,
			Role: string(p.Role), Commit: p.CommitIndex, Log: p.LastIndex(),
		})
	}
}

func (s *Sim) Leader() int {
	for i, p := range s.peers {
		if p.Alive && p.Role == raft.Leader {
			return i
		}
	}
	return -1
}

func (s *Sim) WaitLeader(ms int) int {
	deadline := s.Now + ms
	for s.Now < deadline {
		if id := s.Leader(); id >= 0 {
			return id
		}
		s.Run(20)
	}
	return s.Leader()
}

func (s *Sim) Propose(key, value string) bool {
	id := s.Leader()
	if id < 0 {
		return false
	}
	ok := s.peers[id].Propose(key, value)
	if ok {
		s.persist(id)
		s.Events = append(s.Events, Event{
			T: s.Now, Kind: "propose", Peer: id, Term: s.peers[id].CurrentTerm,
			Role: "leader", Note: key + "=" + value, Log: s.peers[id].LastIndex(),
		})
		for _, m := range s.peers[id].Replicate() {
			s.send(m)
		}
	}
	return ok
}

func (s *Sim) WaitApply(key, value string, ms int) bool {
	deadline := s.Now + ms
	for s.Now < deadline {
		n := 0
		for i, p := range s.peers {
			if !p.Alive {
				continue
			}
			if got, ok := s.sms[i].Get(key); ok && got == value {
				n++
			}
		}
		if n > s.N/2 {
			return true
		}
		s.Run(20)
	}
	return false
}

// Partition blocks every pair that is not in the same group.
func (s *Sim) Partition(groups ...[]int) {
	s.block = map[[2]int]bool{}
	groupOf := make([]int, s.N)
	for i := range groupOf {
		groupOf[i] = -1
	}
	for g, members := range groups {
		for _, id := range members {
			groupOf[id] = g
		}
	}
	for a := 0; a < s.N; a++ {
		for b := 0; b < s.N; b++ {
			if a == b {
				continue
			}
			if groupOf[a] == -1 || groupOf[b] == -1 || groupOf[a] != groupOf[b] {
				s.block[[2]int{a, b}] = true
			}
		}
	}
	s.Events = append(s.Events, Event{T: s.Now, Kind: "partition", Note: fmt.Sprintf("%v", groups)})
}

// Block is a one-way cut, used for asymmetric partitions.
func (s *Sim) Block(from, to int) {
	s.block[[2]int{from, to}] = true
	s.Events = append(s.Events, Event{T: s.Now, Kind: "partition", From: from, To: to, Note: "asymmetric"})
}

func (s *Sim) Heal() {
	s.block = map[[2]int]bool{}
	s.Events = append(s.Events, Event{T: s.Now, Kind: "heal", Note: "partition cleared"})
}

func (s *Sim) Crash(id int) {
	s.peers[id].Alive = false
	s.elecGen[id]++
	s.hbGen[id]++
	s.Events = append(s.Events, Event{
		T: s.Now, Kind: "crash", Peer: id, Term: s.peers[id].CurrentTerm, Role: string(s.peers[id].Role),
	})
}

func (s *Sim) Restart(id int) {
	s.peers[id].Restore(s.disk[id])
	s.sms[id] = machine.New()
	s.Events = append(s.Events, Event{
		T: s.Now, Kind: "restart", Peer: id, Term: s.peers[id].CurrentTerm, Role: "follower",
		Log: s.peers[id].LastIndex(),
	})
	s.armElection(id)
}

func (s *Sim) LeadersInTerm(term int) int {
	seen := map[int]bool{}
	for _, e := range s.Events {
		if e.Kind == "role" && e.Role == "leader" && e.Term == term {
			seen[e.Peer] = true
		}
	}
	return len(seen)
}

func (s *Sim) LogMatchProperty() bool {
	for i := 0; i < s.N; i++ {
		for j := i + 1; j < s.N; j++ {
			a, b := s.peers[i].Log, s.peers[j].Log
			limit := len(a)
			if len(b) < limit {
				limit = len(b)
			}
			for idx := 1; idx < limit; idx++ {
				if a[idx].Term == b[idx].Term {
					for k := 1; k <= idx; k++ {
						if a[k] != b[k] {
							return false
						}
					}
				}
			}
		}
	}
	return true
}

func (s *Sim) Trace() Trace {
	return Trace{Seed: s.Seed, Peers: s.N, Scenario: s.Scenario, Events: s.Events}
}

func (s *Sim) Hash() string {
	b, _ := json.Marshal(s.Events)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (s *Sim) WriteTrace(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	name := filepath.Join(dir, s.Scenario+".json")
	b, err := json.MarshalIndent(s.Trace(), "", "  ")
	if err != nil {
		return "", err
	}
	return name, os.WriteFile(name, b, 0o644)
}

// RunScenario executes a named demo and returns the simulator.
func RunScenario(name string, seed int64) *Sim {
	switch name {
	case "election":
		s := New(seed, 5, name)
		s.WaitLeader(2000)
		s.Run(200)
		return s
	case "crash":
		s := New(seed, 5, name)
		leader := s.WaitLeader(2000)
		s.Propose("k", "v1")
		s.WaitApply("k", "v1", 2000)
		if leader >= 0 {
			s.Crash(leader)
		}
		s.Run(50)
		newLeader := s.WaitLeader(3000)
		if newLeader >= 0 {
			s.Propose("k", "v2")
			s.WaitApply("k", "v2", 2000)
		}
		if leader >= 0 {
			s.Restart(leader)
			s.Run(800)
		}
		return s
	case "partition":
		s := New(seed, 5, name)
		leader := s.WaitLeader(2000)
		s.Propose("a", "1")
		s.WaitApply("a", "1", 2000)
		if leader < 0 {
			return s
		}
		minor := []int{leader}
		major := []int{}
		for i := 0; i < 5; i++ {
			if i == leader {
				continue
			}
			if len(minor) < 2 {
				minor = append(minor, i)
			} else {
				major = append(major, i)
			}
		}
		s.Partition(minor, major)
		s.Run(800)
		// The old leader is in the minority and must not be the one to commit.
		majorityLeader := -1
		deadline := s.Now + 2000
		for s.Now < deadline && majorityLeader < 0 {
			for _, id := range major {
				if s.peers[id].Alive && s.peers[id].Role == raft.Leader {
					majorityLeader = id
				}
			}
			if majorityLeader < 0 {
				s.Run(20)
			}
		}
		if majorityLeader >= 0 {
			s.peers[majorityLeader].Propose("a", "majority")
			s.persist(majorityLeader)
			s.Events = append(s.Events, Event{
				T: s.Now, Kind: "propose", Peer: majorityLeader, Term: s.peers[majorityLeader].CurrentTerm,
				Role: "leader", Note: "a=majority", Log: s.peers[majorityLeader].LastIndex(),
			})
			for _, m := range s.peers[majorityLeader].Replicate() {
				s.send(m)
			}
		}
		s.Run(800)
		s.Heal()
		s.Run(1500)
		return s
	case "asymmetric":
		s := New(seed, 5, name)
		s.WaitLeader(2000)
		// One-way cuts during the next election window.
		s.Block(0, 1)
		s.Block(1, 2)
		s.Block(2, 0)
		s.Crash(s.Leader())
		s.Run(2000)
		return s
	case "conflict":
		s := New(seed, 5, name)
		leader := s.WaitLeader(2000)
		s.Propose("a", "committed")
		s.WaitApply("a", "committed", 2000)
		if leader < 0 {
			return s
		}
		isolated := (leader + 1) % 5
		var rest []int
		for i := 0; i < 5; i++ {
			if i != isolated {
				rest = append(rest, i)
			}
		}
		s.Partition([]int{isolated}, rest)
		// Majority commits a newer value. Isolated cannot.
		s.Propose("a", "newer")
		s.WaitApply("a", "newer", 2000)
		// Force the isolated peer through a term it cannot commit, then heal.
		s.Run(500)
		s.Heal()
		s.Run(1500)
		return s
	default:
		return New(seed, 5, name)
	}
}
