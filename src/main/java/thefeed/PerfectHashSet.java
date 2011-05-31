package thefeed;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: 5/30/11
 * Time: 11:20 AM
 */
public class PerfectHashSet {

  private Set<Long> temp = new HashSet<Long>();
  private int length;

  public void add(long l) {
    temp.add(l);
  }

  // Perfect hash?
  private long[] set;

  public long[] set() {
    for (int start = temp.size(); ; start++) {
      set = new long[start];
      for (Long aLong : temp) {
        int position = (int) (aLong % start);
        if (set[position] != 0) {
          set = null;
          break;
        } else {
          set[position] = aLong;
        }
      }
      if (set != null) {
        length = set.length;
        System.out.println(temp.size() + " < " + start);
        return set;
      }
    }
  }

  public boolean contains(long l) {
    if (set == null) {
      set();
    }
    int key = (int) (l & 0x7FFFFFFF);
    int index = key % length;
    return set[index] == l;
  }
}
