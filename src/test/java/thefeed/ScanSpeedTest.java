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
    List<Entry> list = new LinkedList<Entry>();
    // Put 1 million entries in, scan them
    for (int i = 0; i < TIMES; i++) {
      Entry entry = new Entry();
      list.add(entry);
    }
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
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES * BYTES_PER_ENTRY);
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
    LinkedMemory head = null;
    LinkedMemory current = null;
    for (int j = 0; j < BLOCKS; j++) {
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(TIMES / BLOCKS * BYTES_PER_ENTRY);
      LinkedMemory tmp = current;
      current = new LinkedMemory(byteBuffer, current);
      current.next = tmp;
      head = current;
    }
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

}
