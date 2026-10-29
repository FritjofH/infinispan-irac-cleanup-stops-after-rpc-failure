# Infinispan IRAC tombstone cleanup stops for good after one failed RPC

Reproducer for: one failed RPC in an IRAC tombstone cleanup round stops all later cleanup rounds for that
cache on that node. Nothing is logged above TRACE. Stale IRAC keys and tombstones then grow without bound.

Reproduced on **16.0.14** and **16.3.1**. The code involved is the same on `main`.

## The bug

`DefaultIracTombstoneManager` runs its cleanup rounds through an `IracExecutor`:

```java
iracExecutor = new IracExecutor(this::performCleanup);

private CompletionStage<Void> performCleanup() {
   ...
   scheduler.onTaskStarted(tombstoneMap.size());
   return CompletionStages.allOf(checkStaleKeys(), checkStaleTombstones())
         .whenComplete(scheduler);
}
```

The stage that `performCleanup()` returns completes **exceptionally** when any RPC of the round fails. For
example, `checkKeysExistsInPrimary` or `PrimaryOwnerCheckTask` gets a `TimeoutException`, and
`concatMapDelayError` reports it as a `CompositeException`.

`IracExecutor` chains each round on the previous one:

```java
public void run() {
   if (hasPendingRunnable.compareAndSet(false, true)) {
      lastRunnable = lastRunnable.thenComposeAsync(runnable, executor);
   }
}
// WrappedRunnable.apply():  hasPendingRunnable.set(false); return runnable.get();
```

Once `lastRunnable` has completed exceptionally:

1. `thenComposeAsync` does not call `runnable` again. Every later stage completes with the same exception.
2. `hasPendingRunnable` is reset only inside `runnable`, so it stays `true`, and every later `run()` does nothing.

The scheduler still calls `run()` every round, but no round runs again. On that node, for that cache:

- `checkStaleKeys` (#14360) no longer runs. Keys stranded on backup owners stay in `updatedKeys` for good:
  `queue_size` grows.
- `checkStaleTombstones` no longer runs. Tombstones the node owns as primary are never checked or removed:
  `number_of_tombstones` grows, on the other owners as well.
- `tombstone_cleanup_task_current_delay` freezes at its last value.
- The only log line is `[IRAC] Tombstone cleanup round failed!` at TRACE.

The IRAC send loop in `DefaultIracManager.run()` uses the same `IracExecutor`, but it ends its `Flowable` with
`onErrorComplete`, so its stage never completes exceptionally. The cleanup round has no such guard.

## Running it

JDK 17+ and Maven:

```bash
mvn -q compile dependency:build-classpath -Dmdep.outputFile=cp.txt
java -cp "target/classes:$(cat cp.txt)" repro.IracCleanupStopsRepro                 # with a 6 s stall
java -Dstall=false -cp "target/classes:$(cat cp.txt)" repro.IracCleanupStopsRepro   # control
java -cp "target/classes:$(cat cp.txt)" repro.IracExecutorDeadChain                 # IracExecutor on its own
```

To run against another version, add `-Dinfinispan.version=16.3.1` to the `mvn` command.

### `IracCleanupStopsRepro`

The setup runs in one JVM, bound to 127.0.0.1, in server mode, so non-tx DIST uses
`NonTxDistributionInterceptor` as Infinispan Server does:

- Site A has 4 nodes. Sites B and C have 1 node each, bridged with RELAY2.
- Cache `sessions` is `DIST_ASYNC`, `owners=3`, with ASYNC backups to the other two sites.
- `remote-timeout` and `max-cleanup-delay` are both 1 s, so the effect shows within a minute. With the defaults
  (15 s and 30 s), the behaviour is the same, only slower.

The run has three phases:

1. **Load, 60 s.** Site A writes keys and site B removes them (in production: users update sessions in one site,
   and the session reaper runs in another). Site A builds up IRAC queue entries and tombstones.
2. **Stall, at t=20 s.** Node A2 drops all of its cluster traffic for 6 s (`DISCARD` with `discard_all`). This
   is shorter than failure detection, so nobody leaves the view. Cleanup rounds running during the stall get RPC
   timeouts.
3. **Quiet, 60 s, no writes.** With 1 s cleanup rounds, every node should drain to 0.

Result with the stall, 16.0.14 (16.3.1 gives the same):

```
quiet t=10s   | A1 queue=   2 tombstones= 367 | A2 queue=  10 tombstones= 378 | A3 queue=  23 tombstones= 379 | A4 queue=  21 tombstones= 382 | B1 queue=   0 tombstones=   0 | C1 queue=   0 tombstones=   0
quiet t=60s   | A1 queue=   2 tombstones= 367 | A2 queue=  10 tombstones= 378 | A3 queue=  23 tombstones= 379 | A4 queue=  21 tombstones= 382 | B1 queue=   0 tombstones=   0 | C1 queue=   0 tombstones=   0
tombstone cleanup IracExecutor A1: lastRunnable completedExceptionally=true, hasPendingRunnable=true, cause CompositeException: 29 exceptions occurred.
tombstone cleanup IracExecutor A2: lastRunnable completedExceptionally=true, hasPendingRunnable=true, cause CompositeException: 60 exceptions occurred.
tombstone cleanup IracExecutor A3: lastRunnable completedExceptionally=true, hasPendingRunnable=true, cause CompositeException: 21 exceptions occurred.
tombstone cleanup IracExecutor A4: lastRunnable completedExceptionally=true, hasPendingRunnable=true, cause CompositeException: 25 exceptions occurred.
tombstone cleanup IracExecutor B1: lastRunnable completedExceptionally=false, hasPendingRunnable=false
tombstone cleanup IracExecutor C1: lastRunnable completedExceptionally=false, hasPendingRunnable=false
VERDICT Infinispan 16.0.14, stall=true: LEFT BEHIND (after 60 s quiet: IRAC queue=56, tombstones=1506)
```

The control (`-Dstall=false`) drains:

```
quiet t=10s   | A1 queue=   0 tombstones=   0 | A2 queue=   0 tombstones=   0 | A3 queue=   0 tombstones=   0 | A4 queue=   0 tombstones=   0 | B1 queue=   0 tombstones=   0 | C1 queue=   0 tombstones=   0
VERDICT Infinispan 16.0.14, stall=false: DRAINED (after 60 s quiet: IRAC queue=0, tombstones=0)
```

### `IracExecutorDeadChain`

The same effect with the real `IracExecutor` and nothing else. The second round's stage fails, and the
remaining four `run()` calls execute nothing:

```
round 1 runs
round 2 runs
VERDICT: run() called 6 times, rounds executed = 2 -> executor is dead after the first failed round
```

## Seen in production

The setup is three sites (one site of 6 VMs, two sites in Kubernetes), Infinispan Server 16.0.14, JDK 25. The
two affected caches are `DIST_ASYNC` and `owners=3`, written in one site and removed in another.

The trigger was a gossip router roll and cross-site disruption, followed by restarts of some nodes. After that,
`tombstone_cleanup_task_current_delay` stayed frozen at about 30000 for 7 of 12 cache and node pairs. On those
pairs:

- `queue_size` grew from about 20 to about 1,600 over two days.
- Tombstones reached about 1M per node, also on the healthy nodes, because the stuck nodes are the primaries for
  part of the keys.
- Nothing was logged at INFO. A virtual-thread dump showed no thread inside cleanup.

Restarting the node is the only way to recover.

## Possible fix

Keep the executor chain healthy. Either let a failed round complete normally in `performCleanup()`, for example
by logging the failure at WARN and recovering, or have `IracExecutor.WrappedRunnable` handle an exceptional
stage, so that one failed round cannot stop all later ones.
