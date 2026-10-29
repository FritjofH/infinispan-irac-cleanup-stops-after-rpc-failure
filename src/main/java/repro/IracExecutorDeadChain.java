package repro;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.Version;
import org.infinispan.xsite.irac.IracExecutor;

/**
 * DefaultIracTombstoneManager drives its cleanup rounds through an IracExecutor. A round's stage completes
 * exceptionally when any RPC in it fails. This shows what IracExecutor does with later rounds after that.
 */
public final class IracExecutorDeadChain {

   public static void main(String[] args) throws Exception {
      AtomicInteger rounds = new AtomicInteger();
      IracExecutor executor = new IracExecutor(() -> {
         int n = rounds.incrementAndGet();
         System.out.println("round " + n + " runs");
         // Round 2 fails, like a cleanup round whose RPC timed out.
         CompletionStage<Void> stage = n == 2
               ? CompletableFuture.failedFuture(new org.infinispan.commons.TimeoutException("simulated RPC timeout"))
               : CompletableFuture.completedFuture(null);
         return stage;
      });
      executor.setExecutor(Executors.newSingleThreadExecutor());

      for (int i = 0; i < 6; i++) {
         executor.run();   // what the cleanup scheduler does every round
         TimeUnit.MILLISECONDS.sleep(200);
      }
      System.out.printf("VERDICT Infinispan %s: run() called 6 times, rounds executed = %d%s%n", Version.getVersion(), rounds.get(),
            rounds.get() == 2 ? " -> executor is dead after the first failed round" : "");
      System.exit(0);
   }
}
