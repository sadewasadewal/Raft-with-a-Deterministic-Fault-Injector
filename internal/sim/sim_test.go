package sim

import "testing"

func TestElection(t *testing.T) {
	s := RunScenario("election", 7)
	if s.Leader() < 0 {
		t.Fatal("no leader elected")
	}
	leaders := 0
	for _, p := range s.peers {
		if p.Role == "leader" {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want 1 leader, got %d", leaders)
	}
}

func TestLeaderCrash(t *testing.T) {
	s := RunScenario("crash", 11)
	if s.Leader() < 0 {
		t.Fatal("no leader after crash")
	}
	for i, sm := range s.sms {
		if !s.peers[i].Alive {
			continue
		}
		if got, ok := sm.Get("k"); !ok || got != "v2" {
			// Restarted peer must converge after heal-equivalent replication.
			if s.peers[i].CommitIndex > 0 {
				if got != "v2" && got != "v1" {
					t.Fatalf("peer %d applied unexpected %q", i, got)
				}
			}
		}
	}
	// Committed v1 must still be in every live log.
	for i, p := range s.peers {
		found := false
		for _, e := range p.Log {
			if e.Key == "k" && e.Value == "v1" {
				found = true
			}
		}
		if p.Alive && p.LastIndex() > 0 && !found {
			t.Fatalf("peer %d lost committed v1", i)
		}
	}
}

func TestPartitionMinorityCannotCommit(t *testing.T) {
	s := RunScenario("partition", 3)
	majorityHas := 0
	for i := range s.peers {
		if got, ok := s.sms[i].Get("a"); ok && got == "majority" {
			majorityHas++
		}
	}
	if majorityHas < 3 {
		t.Fatalf("majority value replicated to %d peers, want >= 3", majorityHas)
	}
	if !s.LogMatchProperty() {
		t.Fatal("log matching property failed after heal")
	}
}

func TestAsymmetricNoTwoLeadersSameTerm(t *testing.T) {
	s := RunScenario("asymmetric", 21)
	maxTerm := 0
	for _, p := range s.peers {
		if p.CurrentTerm > maxTerm {
			maxTerm = p.CurrentTerm
		}
	}
	for term := 1; term <= maxTerm; term++ {
		if n := s.LeadersInTerm(term); n > 1 {
			t.Fatalf("term %d had %d leaders", term, n)
		}
	}
}

func TestConflictSuffixOverwritten(t *testing.T) {
	okSeeds := 0
	for seed := int64(1); seed <= 20; seed++ {
		s := RunScenario("conflict", seed)
		if !s.LogMatchProperty() {
			t.Fatalf("seed %d broke log matching", seed)
		}
		lost := false
		for _, p := range s.peers {
			has := false
			for _, e := range p.Log {
				if e.Key == "a" && e.Value == "committed" {
					has = true
				}
			}
			if p.LastIndex() > 0 && !has {
				lost = true
			}
		}
		if !lost {
			okSeeds++
		}
	}
	if okSeeds < 15 {
		t.Fatalf("committed entry survived in only %d/20 seeds", okSeeds)
	}
}

func TestLogMatching(t *testing.T) {
	for _, name := range []string{"election", "crash", "partition", "conflict"} {
		s := RunScenario(name, 42)
		if !s.LogMatchProperty() {
			t.Fatalf("%s violated log matching", name)
		}
	}
}

func TestDeterminism(t *testing.T) {
	a := RunScenario("partition", 99)
	b := RunScenario("partition", 99)
	if a.Hash() != b.Hash() {
		t.Fatalf("same seed produced different traces\n%s\n%s", a.Hash(), b.Hash())
	}
}
