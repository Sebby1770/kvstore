-------------------------------- MODULE QuorumKV --------------------------------
(***************************************************************************)
(* KVStore — an abstract model of the quorum register for a single key:    *)
(*                                                                          *)
(*   - N replicas, each holding a versioned value (last-writer-wins).       *)
(*   - A write picks any W-sized quorum, stamps a fresh, strictly-          *)
(*     increasing version, and updates those replicas. It is "committed".   *)
(*   - A read picks any R-sized quorum and returns the freshest version it  *)
(*     sees.                                                                 *)
(*                                                                          *)
(* The safety property (Consistency): when R + W > N, every read returns a  *)
(* version at least as new as the newest committed write. This is exactly   *)
(* the guarantee the Go coordinator relies on (and read-repair preserves).  *)
(*                                                                          *)
(* Quantifying over ALL W- and R-quorums models every possible failure     *)
(* pattern: a node being down just means some quorums can't form, but the   *)
(* invariant must hold for whichever quorums DO form.                       *)
(*                                                                          *)
(* Check with TLC against QuorumKV.cfg — see spec/README.md.                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes,   \* the replica set, e.g. {n1, n2, n3}
          Values,  \* the values that may be written, e.g. {a, b}
          R,       \* read quorum
          W,       \* write quorum
          MaxVer   \* bound on versions so the model is finite

ASSUME RWInRange == (R \in 1..Cardinality(Nodes)) /\ (W \in 1..Cardinality(Nodes))
ASSUME Overlap   == R + W > Cardinality(Nodes)   \* the R + W > N condition

VARIABLES store,      \* [Nodes -> [ver, val]]   each replica's current entry
          nextVer,    \* the next version to assign
          committed,  \* set of [ver, val] writes that reached a W-quorum
          lastRead,   \* the entry the most recent read returned
          readOK      \* did every read so far satisfy Consistency?

vars == <<store, nextVer, committed, lastRead, readOK>>

Vers    == 0..MaxVer
NoEntry == [ver |-> 0, val |-> "none"]

TypeOK ==
  /\ store \in [Nodes -> [ver: Vers, val: Values \cup {"none"}]]
  /\ nextVer \in 1..(MaxVer + 1)
  /\ committed \in SUBSET [ver: Vers, val: Values]
  /\ lastRead \in [ver: Vers, val: Values \cup {"none"}]
  /\ readOK \in BOOLEAN

Init ==
  /\ store = [n \in Nodes |-> NoEntry]
  /\ nextVer = 1
  /\ committed = {}
  /\ lastRead = NoEntry
  /\ readOK = TRUE

QuorumsOf(k) == {Q \in SUBSET Nodes : Cardinality(Q) = k}

MaxCommittedVer ==
  IF committed = {} THEN 0
  ELSE CHOOSE m \in {e.ver : e \in committed} :
         \A k \in {e.ver : e \in committed} : m >= k

\* A write to a W-quorum, stamped with the next (strictly larger) version.
Write(v, Q) ==
  /\ nextVer <= MaxVer
  /\ store' = [n \in Nodes |->
                 IF n \in Q THEN [ver |-> nextVer, val |-> v] ELSE store[n]]
  /\ committed' = committed \cup {[ver |-> nextVer, val |-> v]}
  /\ nextVer' = nextVer + 1
  /\ UNCHANGED <<lastRead, readOK>>

\* The freshest entry seen across a quorum.
Freshest(Q) ==
  CHOOSE e \in {store[n] : n \in Q} :
    \A f \in {store[m] : m \in Q} : e.ver >= f.ver

\* A read from an R-quorum; records whether it saw the newest committed write.
Read(Q) ==
  /\ lastRead' = Freshest(Q)
  /\ readOK' = (Freshest(Q).ver >= MaxCommittedVer)
  /\ UNCHANGED <<store, nextVer, committed>>

Next ==
  \/ \E v \in Values, Q \in QuorumsOf(W) : Write(v, Q)
  \/ \E Q \in QuorumsOf(R) : Read(Q)

Spec == Init /\ [][Next]_vars

\* Safety: no read ever returned older than the newest committed write.
Consistency == readOK = TRUE
================================================================================
