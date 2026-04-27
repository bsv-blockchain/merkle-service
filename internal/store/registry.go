package store

// Registry bundles every store needed by a service. Services read the fields
// they need (typed as interfaces) so they stay backend-agnostic. Close()
// releases any backend-owned resources (Aerospike client, SQL connection pool,
// background sweepers).
type Registry struct {
	Registration        RegistrationStore
	Stump               StumpStore
	Subtree             SubtreeStore
	CallbackDedup       CallbackDedupStore
	CallbackURLRegistry CallbackURLRegistry
	CallbackAccumulator CallbackAccumulatorStore
	SeenCounter         SeenCounterStore
	SubtreeCounter      SubtreeCounterStore

	// Health reports backend reachability for the API health endpoint. May be
	// nil for backends that don't need a liveness probe (e.g., an in-memory
	// test backend).
	Health BackendHealth

	// closers are invoked in reverse order by Close. Backends register any
	// resources that need to be released here.
	closers []func() error
}

// AddCloser registers a cleanup function. Invoked by Close in reverse order.
func (r *Registry) AddCloser(fn func() error) {
	if fn != nil {
		r.closers = append(r.closers, fn)
	}
}

// Close releases backend-owned resources. Closers run in reverse order. Errors
// are collected and returned as a joined error so one failure doesn't hide
// another.
func (r *Registry) Close() error {
	var firstErr error
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i](); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.closers = nil
	return firstErr
}
