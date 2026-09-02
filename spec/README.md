# Formal spec — quorum safety

[`QuorumKV.tla`](QuorumKV.tla) is a TLA+ model of KVStore's core guarantee for
a single key: with **R + W > N**, every read returns a value at least as new as
the newest committed write (a write that reached a W-quorum). This is the
property the Go coordinator depends on and that read-repair preserves.

The model quantifies over *every* W-quorum and *every* R-quorum, so it covers
all failure patterns at once: a node being down just means some quorums can't
form, but the invariant must hold for whichever quorums do.

## Checking it

With [TLA+ tools](https://github.com/tlaplus/tlaplus) (`tla2tools.jar`, needs Java):

```sh
java -cp tla2tools.jar tlc2.TLC -config QuorumKV.cfg QuorumKV.tla
```

The default config is N = 3, R = W = 2 (so R + W = 4 > 3). Result:

```
Model checking completed. No error has been found.
373 distinct states found, 0 states left on queue.
```

## It has teeth

The guarantee genuinely depends on R + W > N. Drop the `ASSUME Overlap` line and
re-check with R = 1, W = 2 (so R + W = 3 = N) and TLC finds a counterexample:

```
Error: Invariant Consistency is violated.
```

— a read quorum that misses every replica the latest write touched. That's the
exact reason the gateway warns when `R + W <= RF`.

> Both runs above were executed with TLC (openjdk 18). The deterministic
> simulation in [`internal/cluster/cluster_sim_test.go`](../kvgw/internal/cluster/cluster_sim_test.go)
> checks the *implementation* against the same idea; this checks the *protocol*.
