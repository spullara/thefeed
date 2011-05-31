package thefeed;

import thefeed.mahout.FastIDSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Calibrate a platform to see how quickly it can scan entries. Run with -server and 2G of memory.
 * <p/>
 * User: sam
 * Date: 5/22/11
 * Time: 2:00 PM
 */
public class Calibrate {

  private static final int FOLLOWEES = 1000;

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Random r = new Random();
    FastIDSet comparisons = new FastIDSet(10000);
    for (int i = 0; i < FOLLOWEES; i++) {
      comparisons.add((long) r.nextInt(RANGE));
    }
    LinkedFeed head = null;
    LinkedFeed current = null;
    for (int j = 0; j < BLOCKS; j++) {
      long[] byteBuffer = new long[(TIMES / BLOCKS * 2)];
      LinkedFeed tmp = current;
      current = new LinkedFeed(byteBuffer, current);
      current.next = tmp;
      head = current;
      for (int i = 0; i < TIMES / BLOCKS * 2; i += 2) {
        byteBuffer[i] = r.nextInt(RANGE);
      }
    }
    System.out.println("CORES,TOTAL,PERCORE,HITS");
    for (int cpus = 1; cpus <= Runtime.getRuntime().availableProcessors()*2; cpus++) {
      ExecutorService es = Executors.newCachedThreadPool();
      List<Callable<Void>> runs = new ArrayList<Callable<Void>>();
      final LinkedFeed finalHead = head;
      final AtomicInteger hits = new AtomicInteger(0);
      for (int i = 0; i < cpus; i++) {
        final FastIDSet finalComparisons = comparisons;
        runs.add(new Callable<Void>() {
          @Override
          public Void call() {
            for (LinkedFeed current = finalHead; current != null; current = current.next) {
              long[] value = current.value;
              for (int i = 0; i < TIMES / BLOCKS * 2; i += 2) {
                if (finalComparisons.contains(value[i])) {
                  hits.incrementAndGet();
                }
              }
            }
            return null;
          }
        });
      }
      long start = System.currentTimeMillis();
      for (Future<Void> run : es.invokeAll(runs)) {
        run.get();
      }
      long result = TIMES / (System.currentTimeMillis() - start);
      es.shutdownNow();
      System.out.println(cpus + "," + cpus * result + "," + result + "," + hits);
    }
  }

  private static int RANGE = 100000;
  private static int BLOCKS = 5000;
  private static int TIMES = 30000000;

  /**
   * We create the feed by linking together reverse chronological epochs of entries.
   * <p/>
   * User: sam
   * Date: 5/22/11
   * Time: 2:01 PM
   */
  private static class LinkedFeed {
    long[] value;
    LinkedFeed next;

    public LinkedFeed(long[] value, LinkedFeed next) {
      this.value = value;
      this.next = next;
    }
  }
}
