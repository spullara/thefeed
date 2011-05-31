package thefeed;

import thefeed.mahout.FastIDSet;

import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Calibrate a platform to see how quickly it can scan entries. Run with -server and 2G of memory.
 * <p/>
 * User: sam
 * Date: 5/22/11
 * Time: 2:00 PM
 */
public class SingleThreaded {

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
      long[] feed = new long[(TIMES / BLOCKS * 2)];
      LinkedFeed tmp = current;
      current = new LinkedFeed(feed, current);
      current.next = tmp;
      head = current;
      for (int i = 0; i < TIMES / BLOCKS * 2; i += 2) {
        feed[i] = r.nextInt(RANGE);
      }
    }
    System.out.println("TOTAL,HITS");
    for (int i = 0; i < 3; i++) {
      long start = System.currentTimeMillis();
      int hits = 0;
      for (current = head; current != null; current = current.next) {
        long[] value = current.value;
        for (int j = 0; j < TIMES / BLOCKS * 2; j += 2) {
          if (comparisons.contains(value[j])) {
            hits++;
          }
        }
      }
      long result = TIMES / (System.currentTimeMillis() - start);
      System.out.println(result + "," + hits);
    }
  }

  private static int RANGE = 100000;
  private static int BLOCKS = 10;
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
