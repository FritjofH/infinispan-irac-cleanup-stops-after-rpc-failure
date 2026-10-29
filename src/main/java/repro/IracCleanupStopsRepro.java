package repro;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.xsite.irac.DefaultIracManager;
import org.infinispan.xsite.irac.IracManager;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;

/**
 * Three sites: A (4 nodes), B and C (1 node each). Cache "sessions" is DIST_ASYNC, owners=3, with ASYNC
 * backups to the other two sites. A writes keys, B removes them, so A builds an IRAC queue and tombstones.
 * <p>
 * During the load, node A2 drops all its cluster traffic for a few seconds (JGroups DISCARD), shorter than
 * failure detection, so nobody leaves the view. Tombstone cleanup rounds running at that moment get an RPC
 * failure. After the load stops, every node's IRAC queue and tombstones should drain within a few cleanup rounds.
 * <p>
 * Run with -Dstall=false for the control.
 */
public final class IracCleanupStopsRepro {

   private static final boolean STALL = Boolean.parseBoolean(System.getProperty("stall", "true"));
   private static final int STALL_AT = Integer.getInteger("stall.at", 20);
   private static final int STALL_SECONDS = Integer.getInteger("stall.seconds", 6);
   private static final int LOAD_SECONDS = Integer.getInteger("load.seconds", 60);
   private static final int QUIET_SECONDS = Integer.getInteger("quiet.seconds", 60);
   private static final int KEYS = Integer.getInteger("keys", 5000);
   private static final int A_NODES = 4;

   public static void main(String[] args) throws Exception {
      System.setProperty("java.net.preferIPv4Stack", "true");
      System.setProperty("jgroups.bind.address", "127.0.0.1");
      System.out.printf(Locale.ROOT, "Infinispan %s, JDK %s, stall=%s%n", Version.getVersion(), System.getProperty("java.version"), STALL);

      String aHosts = IntStream.range(0, A_NODES).mapToObj(n -> "127.0.0.1[" + (7800 + n) + "]").collect(Collectors.joining(","));
      Map<String, EmbeddedCacheManager> nodes = new LinkedHashMap<>();
      for (int i = 0; i < A_NODES; i++) {
         nodes.put("A" + (i + 1), startNode("A", "A" + (i + 1), 7800 + i, 7900 + i, aHosts));
      }
      nodes.put("B1", startNode("B", "B1", 7810, 7910, "127.0.0.1[7810]"));
      nodes.put("C1", startNode("C", "C1", 7820, 7920, "127.0.0.1[7820]"));
      Map<String, Cache<String, String>> caches = new LinkedHashMap<>();
      nodes.forEach((name, cm) -> caches.put(name, cm.getCache("sessions")));
      for (String master : List.of("A1", "B1", "C1")) {
         awaitSites(master, nodes.get(master));
      }
      System.out.println("Sites connected: " + transport(nodes.get("A1")).getSitesView());

      List<Cache<String, String>> siteA = new ArrayList<>();
      for (int i = 1; i <= A_NODES; i++) {
         siteA.add(caches.get("A" + i));
      }
      Cache<String, String> siteB = caches.get("B1");
      AtomicBoolean running = new AtomicBoolean(true);
      LongAdder writes = new LongAdder();
      LongAdder removes = new LongAdder();
      ExecutorService pool = Executors.newFixedThreadPool(6);
      for (int t = 0; t < 4; t++) {
         pool.submit(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (running.get()) {
               try {
                  siteA.get(rnd.nextInt(A_NODES)).put("key-" + rnd.nextInt(KEYS), "value-" + rnd.nextLong());
                  writes.increment();
               } catch (Exception e) {
                  // writes routed through A2 time out during the stall
               }
            }
            return null;
         });
      }
      for (int t = 0; t < 2; t++) {
         pool.submit(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (running.get()) {
               siteB.remove("key-" + rnd.nextInt(KEYS));
               removes.increment();
               TimeUnit.MICROSECONDS.sleep(200);
            }
            return null;
         });
      }

      System.out.printf(Locale.ROOT, "--- load: A writes, B removes, %d s ---%n", LOAD_SECONDS);
      for (int s = 5; s <= LOAD_SECONDS; s += 5) {
         TimeUnit.SECONDS.sleep(5);
         print("load t=" + s + "s", caches);
         if (STALL && s == STALL_AT) {
            stall(nodes.get("A2"));
            System.out.println(">>> A2 traffic restored; A view still " + transport(nodes.get("A1")).getMembers());
         }
      }
      running.set(false);
      pool.shutdown();
      pool.awaitTermination(30, TimeUnit.SECONDS);
      System.out.printf(Locale.ROOT, "--- quiet: no writes, %d s (%,d writes on A, %,d removes on B) ---%n", QUIET_SECONDS,
            writes.sum(), removes.sum());
      for (int s = 10; s <= QUIET_SECONDS; s += 10) {
         TimeUnit.SECONDS.sleep(10);
         print("quiet t=" + s + "s", caches);
      }

      System.out.println();
      caches.forEach((name, cache) -> System.out.println("tombstone cleanup IracExecutor " + name + ": " + cleanupExecutorState(cache)));
      long queue = caches.values().stream().mapToLong(c -> irac(c).getQueueSize()).sum();
      long tombstones = caches.values().stream().mapToLong(c -> irac(c).getNumberOfTombstones()).sum();
      System.out.printf(Locale.ROOT, "VERDICT Infinispan %s, stall=%s: %s (after %d s quiet: IRAC queue=%d, tombstones=%d)%n",
            Version.getVersion(), STALL, queue == 0 && tombstones == 0 ? "DRAINED" : "LEFT BEHIND", QUIET_SECONDS, queue, tombstones);

      nodes.values().forEach(EmbeddedCacheManager::stop);
      System.exit(0);
   }

   /** Drops all of the node's cluster traffic for a few seconds, shorter than failure detection. */
   private static void stall(EmbeddedCacheManager node) throws Exception {
      JChannel channel = ((JGroupsTransport) transport(node)).getChannel();
      DISCARD discard = new DISCARD();
      discard.discardAll(true);
      channel.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
      System.out.println(">>> A2 drops all traffic for " + STALL_SECONDS + " s");
      TimeUnit.SECONDS.sleep(STALL_SECONDS);
      channel.getProtocolStack().removeProtocol(DISCARD.class);
   }

   private static void print(String label, Map<String, Cache<String, String>> caches) {
      StringBuilder sb = new StringBuilder(String.format(Locale.ROOT, "%-13s", label));
      caches.forEach((name, cache) -> {
         DefaultIracManager irac = irac(cache);
         sb.append(String.format(Locale.ROOT, " | %s queue=%4d tombstones=%4d", name, irac.getQueueSize(), irac.getNumberOfTombstones()));
      });
      System.out.println(sb);
   }

   /** DefaultIracTombstoneManager chains every cleanup round on IracExecutor.lastRunnable. */
   private static String cleanupExecutorState(Cache<?, ?> cache) {
      try {
         Object tombstoneManager = ComponentRegistry.componentOf(cache, IracTombstoneManager.class);
         Object executor = field(tombstoneManager, "iracExecutor");
         CompletableFuture<?> last = (CompletableFuture<?>) field(executor, "lastRunnable");
         String cause = "";
         if (last.isCompletedExceptionally()) {
            try {
               last.join();
            } catch (Exception e) {
               Throwable t = e.getCause() != null ? e.getCause() : e;
               cause = ", cause " + t.getClass().getSimpleName() + ": " + t.getMessage();
            }
         }
         return "lastRunnable completedExceptionally=" + last.isCompletedExceptionally()
               + ", hasPendingRunnable=" + field(executor, "hasPendingRunnable") + cause;
      } catch (ReflectiveOperationException e) {
         return "n/a (" + e + ")";
      }
   }

   private static Object field(Object target, String name) throws ReflectiveOperationException {
      Field f = target.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(target);
   }

   private static DefaultIracManager irac(Cache<?, ?> cache) {
      return (DefaultIracManager) ComponentRegistry.componentOf(cache, IracManager.class);
   }

   private static EmbeddedCacheManager startNode(String site, String node, int localPort, int bridgePort, String localHosts)
         throws IOException {
      String xml;
      try (InputStream in = IracCleanupStopsRepro.class.getResourceAsStream("/site-template.xml")) {
         xml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      String backups = List.of("A", "B", "C").stream().filter(s -> !s.equals(site))
            .map(s -> "        <backup site=\"" + s + "\" strategy=\"ASYNC\" timeout=\"2s\" failure-policy=\"WARN\"/>")
            .collect(Collectors.joining("\n"));
      xml = xml.replace("@SITE@", site).replace("@NODE@", node)
            .replace("@LOCAL_PORT@", String.valueOf(localPort)).replace("@LOCAL_HOSTS@", localHosts)
            .replace("@BRIDGE_PORT@", String.valueOf(bridgePort)).replace("@BACKUPS@", backups)
            .replace("@REMOTE_TIMEOUT@", System.getProperty("remote.timeout", "1000"))
            .replace("@CLEANUP_DELAY@", System.getProperty("cleanup.delay", "1000"));
      ConfigurationBuilderHolder holder = new ParserRegistry().parse(xml);
      // Infinispan Server runs in server mode: non-tx DIST uses NonTxDistributionInterceptor, as in production.
      holder.getGlobalConfigurationBuilder().addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      return new DefaultCacheManager(holder, true);
   }

   private static void awaitSites(String name, EmbeddedCacheManager cm) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (transport(cm).getSitesView() == null || transport(cm).getSitesView().size() < 3) {
         if (System.nanoTime() > deadline) {
            throw new IllegalStateException(name + " sites view: " + transport(cm).getSitesView());
         }
         TimeUnit.MILLISECONDS.sleep(100);
      }
   }

   private static Transport transport(EmbeddedCacheManager cm) {
      return GlobalComponentRegistry.componentOf(cm, Transport.class);
   }
}
