package thefeed;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Some benchmarks for upper limits on scanning speed
 * <p/>
 * User: sam
 * Date: 5/21/11
 * Time: 10:02 PM
 */
public class ScanSpeedTest {

  private static final int TIMES = 20000000;
  private static final int BLOCKS = 5000;
  private static final int BYTES_PER_ENTRY = 16;

  public static class Entry {
    long userid;
    long postid;
  }

  @Test
  public void testLinkedList() {
    System.gc();
    long free = Runtime.getRuntime().freeMemory();
    List<Entry> list = new LinkedList<Entry>();
    // Put 1 million entries in, scan them
    for (int i = 0; i < TIMES; i++) {
      Entry entry = new Entry();
      list.add(entry);
    }
    System.gc();
    System.out.println((free - Runtime.getRuntime().freeMemory()) / TIMES);
    {
      long start = System.currentTimeMillis();
      for (Entry entry : list) {
      }
      System.out.println(TIMES/(System.currentTimeMillis() - start) + " per ms");
    }
  }

  static class LinkedEntry {
    List<Entry> value;
    LinkedEntry next;

    public LinkedEntry(List<Entry> value, LinkedEntry next) {
      this.value = value;
      this.next = next;
    }
  }

  @Test
  public void testLinkedArrayLists() {
    System.gc();
    long free = Runtime.getRuntime().freeMemory();
    LinkedEntry head = null;
    // Put 1 million entries in, scan them
    for (int j = 0; j < BLOCKS; j++) {
      List<Entry> sublist = new ArrayList<Entry>(TIMES/BLOCKS);
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        Entry entry = new Entry();
        sublist.add(entry);
      }
      head = new LinkedEntry(sublist, head);
    }
    System.gc();
    System.out.println((free - Runtime.getRuntime().freeMemory())/TIMES);
    {
      long start = System.currentTimeMillis();
      for (LinkedEntry current = head; current != null; current = current.next) {
        for (Entry entry : current.value) {
        }
      }
      System.out.println(TIMES/(System.currentTimeMillis() - start) + " per ms");
    }
  }

  @Test
  public void testDirectMemory() {
    System.gc();
    long free = Runtime.getRuntime().freeMemory();
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES * BYTES_PER_ENTRY);
    System.gc();
    System.out.println((free - Runtime.getRuntime().freeMemory())/TIMES);
    long start = System.currentTimeMillis();
    for (int i = 0; i < TIMES; i++) {
      byteBuffer.getLong(i* BYTES_PER_ENTRY);
      byteBuffer.getLong(i* BYTES_PER_ENTRY +8);
    }
    for (int i = 0; i < TIMES; i++) {
      byteBuffer.getLong(i* BYTES_PER_ENTRY);
      byteBuffer.getLong(i* BYTES_PER_ENTRY +8);
    }
    for (int i = 0; i < TIMES; i++) {
      byteBuffer.getLong(i* BYTES_PER_ENTRY);
      byteBuffer.getLong(i* BYTES_PER_ENTRY +8);
    }
    System.out.println(3*TIMES/(System.currentTimeMillis() - start) + " per ms");
  }

  static class LinkedMemory {
    ByteBuffer value;
    LinkedMemory next;

    public LinkedMemory(ByteBuffer value, LinkedMemory next) {
      this.value = value;
      this.next = next;
    }
  }

  @Test
  public void testLinkedListDirectMemory() {
    System.gc();
    long free = Runtime.getRuntime().freeMemory();
    LinkedMemory head = null;
    LinkedMemory current = null;
    for (int j = 0; j < BLOCKS; j++) {
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES / BLOCKS * BYTES_PER_ENTRY);
      LinkedMemory tmp = current;
      current = new LinkedMemory(byteBuffer, current);
      current.next = tmp;
      head = current;
    }
    System.gc();
    System.out.println((free - Runtime.getRuntime().freeMemory())/TIMES);
    long start = System.currentTimeMillis();
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        current.value.getLong(i * BYTES_PER_ENTRY);
        current.value.getLong(i * BYTES_PER_ENTRY + 8);
      }
    }
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        current.value.getLong(i * BYTES_PER_ENTRY);
        current.value.getLong(i * BYTES_PER_ENTRY + 8);
      }
    }
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        current.value.getLong(i * BYTES_PER_ENTRY);
        current.value.getLong(i * BYTES_PER_ENTRY + 8);
      }
    }
    System.out.println(3*TIMES/(System.currentTimeMillis() - start) + " per ms");
  }

  @Test
  public void testLongDirectMemory() {
    LongBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES * BYTES_PER_ENTRY).asLongBuffer();
    long start = System.currentTimeMillis();
    for (int i = 0; i < TIMES; i++) {
      byteBuffer.get(i);
      byteBuffer.get(i + 1);
    }
    for (int i = 0; i < TIMES; i++) {
      byteBuffer.get(i);
      byteBuffer.get(i + 1);
    }
    for (int i = 0; i < TIMES; i++) {
      byteBuffer.get(i);
      byteBuffer.get(i + 1);
    }
    System.out.println(3*TIMES/(System.currentTimeMillis() - start) + " per ms");
  }

  static class LinkedLongMemory {
    LongBuffer value;
    LinkedLongMemory next;

    public LinkedLongMemory(LongBuffer value, LinkedLongMemory next) {
      this.value = value;
      this.next = next;
    }
  }

  @Test
  public void testLinkedListLongDirectMemory() {
    LinkedLongMemory head = null;
    LinkedLongMemory current = null;
    for (int j = 0; j < BLOCKS; j++) {
      LongBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES / BLOCKS * BYTES_PER_ENTRY).asLongBuffer();
      LinkedLongMemory tmp = current;
      current = new LinkedLongMemory(byteBuffer, current);
      current.next = tmp;
      head = current;
    }
    long start = System.currentTimeMillis();
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        current.value.get(i);
        current.value.get(i + 1);
      }
    }
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        current.value.get(i);
        current.value.get(i + 1);
      }
    }
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        current.value.get(i);
        current.value.get(i + 1);
      }
    }
    System.out.println(3*TIMES/(System.currentTimeMillis() - start) + " per ms");
  }

  @Test
  public void testCompareLinkedListLongDirectMemory() {
    Random r = new Random();
    Set<Long> comparisons = new HashSet<Long>();
    for (int i = 0; i < 1000; i++) {
      comparisons.add(r.nextLong() % 100000);
    }
    LinkedLongMemory head = null;
    LinkedLongMemory current = null;
    for (int j = 0; j < BLOCKS; j++) {
      LongBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES / BLOCKS * BYTES_PER_ENTRY).asLongBuffer();
      LinkedLongMemory tmp = current;
      current = new LinkedLongMemory(byteBuffer, current);
      current.next = tmp;
      head = current;
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        byteBuffer.put(0, r.nextLong() % 100000);
      }
    }
    int hits = 0;
    long start = System.currentTimeMillis();
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        if (comparisons.contains(current.value.get(i))) {
          current.value.get(i + 1);
          hits++;
        }
      }
    }
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        if (comparisons.contains(current.value.get(i))) {
          current.value.get(i + 1);
          hits++;
        }
      }
    }
    for (current = head; current != null; current = current.next) {
      for (int i = 0; i < TIMES/BLOCKS; i++) {
        if (comparisons.contains(current.value.get(i))) {
          current.value.get(i + 1);
          hits++;
        }
      }
    }
    System.out.println(hits);
    System.out.println(3*TIMES/(System.currentTimeMillis() - start) + " per ms");
  }

  @Test
  public void testConcurrentCompareLinkedListLongDirectMemory() throws InterruptedException, ExecutionException {
    Random r = new Random();
    final Set<Long> comparisons = new HashSet<Long>();
    for (int i = 0; i < 1000; i++) {
      comparisons.add(r.nextLong() % 100000);
    }
    LinkedLongMemory head = null;
    LinkedLongMemory current = null;
    for (int j = 0; j < BLOCKS; j++) {
      LongBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES / BLOCKS * BYTES_PER_ENTRY).asLongBuffer();
      LinkedLongMemory tmp = current;
      current = new LinkedLongMemory(byteBuffer, current);
      current.next = tmp;
      head = current;
      for (int i = 0; i < TIMES/BLOCKS*2; i+=2) {
        byteBuffer.put(i, r.nextLong() % 100000);
      }
    }
    ExecutorService es = Executors.newCachedThreadPool();
    List<Callable<Void>> runs = new ArrayList<Callable<Void>>();
    final LinkedLongMemory finalHead = head;
    int cpus = Runtime.getRuntime().availableProcessors();
    final AtomicInteger hits = new AtomicInteger(0);
    for (int i = 0; i < cpus; i++) {
      runs.add(new Callable<Void>() {
        @Override
        public Void call() {
          for (LinkedLongMemory current = finalHead; current != null; current = current.next) {
            for (int i = 0; i < TIMES/BLOCKS*2; i+=2) {
              if (comparisons.contains(current.value.get(i))) {
                current.value.get(i + 1);
                hits.incrementAndGet();
              }
            }
          }
          return null;
        }
      });
    }
    long start = System.currentTimeMillis();
    for (Future<Void> run : es.invokeAll(runs)){
      run.get();
    }
    System.out.println(hits);
    System.out.println(cpus*TIMES/(System.currentTimeMillis() - start) + " per ms");
  }

}
