package machine

// KV is the replicated state machine: a single-register map applied in log order.
type KV struct {
	Data map[string]string
	Ops  int
}

func New() *KV {
	return &KV{Data: map[string]string{}}
}

func (k *KV) Apply(key, value string) {
	if k.Data == nil {
		k.Data = map[string]string{}
	}
	k.Data[key] = value
	k.Ops++
}

func (k *KV) Get(key string) (string, bool) {
	v, ok := k.Data[key]
	return v, ok
}
