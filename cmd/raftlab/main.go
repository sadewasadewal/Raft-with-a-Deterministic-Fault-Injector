package main

import (
	"flag"
	"fmt"
	"os"

	"raftlab/internal/sim"
)

func main() {
	fs := flag.NewFlagSet("raftlab", flag.ExitOnError)
	seed := fs.Int64("seed", 7, "simulator seed")
	out := fs.String("out", "traces", "directory for trace json")
	args := os.Args[1:]
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "-seed" || a == "-out" || a == "--seed" || a == "--out" {
			if i+1 >= len(args) {
				fmt.Fprintln(os.Stderr, a, "needs a value")
				os.Exit(2)
			}
			_ = fs.Set(a[1:], args[i+1])
			if a[1] == '-' {
				_ = fs.Set(a[2:], args[i+1])
			}
			i++
			continue
		}
		positional = append(positional, a)
	}
	name := "partition"
	if len(positional) > 0 {
		name = positional[0]
	}
	switch name {
	case "election", "crash", "partition", "asymmetric", "conflict":
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\nuse election, crash, partition, asymmetric, conflict\n", name)
		os.Exit(2)
	}
	s := sim.RunScenario(name, *seed)
	path, err := s.WriteTrace(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("scenario=%s seed=%d leader=%d events=%d trace=%s hash=%s\n",
		name, *seed, s.Leader(), len(s.Events), path, s.Hash()[:12])
}
