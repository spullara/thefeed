/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package thefeed.mahout;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @see FastByIDMap
 */
public final class FastIDSet implements Serializable, Cloneable {
  
  private static final double ALLOWED_LOAD_FACTOR = 1.5;
  
  /** Dummy object used to represent a key that has been removed. */
  private static final long REMOVED = Long.MAX_VALUE;
  private static final long NULL = Long.MIN_VALUE;
  
  private long[] keys;
  private int numEntries;
  private int numSlotsUsed;
  
  /** Creates a new  with default capacity. */
  public FastIDSet() {
    this(2);
  }
  
  public FastIDSet(int size) {
    int hashSize = nextTwinPrime((int) (ALLOWED_LOAD_FACTOR * size));
    keys = new long[hashSize];
    Arrays.fill(keys, NULL);
  }

  public static final int MAX_INT_SMALLER_TWIN_PRIME = 2147482949;

  public static int nextTwinPrime(int n) {
    if (n > MAX_INT_SMALLER_TWIN_PRIME) {
      throw new IllegalArgumentException();
    }
    if (n <= 3) {
      return 5;
    }
    int next = nextPrime(n);
    while (isNotPrime(next + 2)) {
      next = nextPrime(next + 4);
    }
    return next + 2;
  }

  /**
   * <p>
   * Finds smallest prime p such that p is greater than or equal to n.
   * </p>
   */
  public static int nextPrime(int n) {
    if (n <= 2) {
      return 2;
    }
    // Make sure the number is odd. Is this too clever?
    n |= 0x1;
    // There is no problem with overflow since Integer.MAX_INT is prime, as it happens
    while (isNotPrime(n)) {
      n += 2;
    }
    return n;
  }

  /** @return <code>true</code> iff n is not a prime */
  public static boolean isNotPrime(int n) {
    if (n < 2 || (n & 0x1) == 0) { // < 2 or even
      return n != 2;
    }
    int max = 1 + (int) Math.sqrt(n);
    for (int d = 3; d <= max; d += 2) {
      if (n % d == 0) {
        return true;
      }
    }
    return false;
  }

  public AtomicInteger jumps = new AtomicInteger(0);

  /**
   * @see #findForAdd(long)
   */
  private int find(long key) {
    int theHashCode = (int) key & 0x7FFFFFFF; // make sure it's positive
    long[] keys = this.keys;
    int hashSize = keys.length;
    int index = theHashCode % hashSize;
    long currentKey = keys[index];
    int jump = 0;
    while ((currentKey != NULL) && (key != currentKey)) { // note: true when currentKey == REMOVED
      if (jump == 0) jump = 1 + theHashCode % (hashSize - 2);
      if (index < jump) {
        index += hashSize - jump;
      } else {
        index -= jump;
      }
      currentKey = keys[index];
    }
    return index;
  }
  
  /**
   * @see #find(long)
   */
  private int findForAdd(long key) {
    long[] keys = this.keys;
    int hashSize = keys.length;
    int theHashCode = (int) key & 0x7FFFFFFF; // make sure it's positive
    int jump = 1 + theHashCode % (hashSize - 2);
    int index = theHashCode % hashSize;
    long currentKey = keys[index];
    while ((currentKey != NULL) && (currentKey != REMOVED) && (key != currentKey)) { // Different
                                                                                                         // here
      if (index < jump) {
        index += hashSize - jump;
      } else {
        index -= jump;
      }
      currentKey = keys[index];
    }
    return index;
  }

  public long[] getKeys() {
    return keys;
  }

  public int size() {
    return numEntries;
  }
  
  public boolean isEmpty() {
    return numEntries == 0;
  }
  
  public boolean contains(long key) {
    return (key != NULL) && (key != REMOVED) && (keys[find(key)] != NULL);
  }
  
  public boolean add(long key) {
    // If less than half the slots are open, let's clear it up
    if (numSlotsUsed * ALLOWED_LOAD_FACTOR >= keys.length) {
      // If over half the slots used are actual entries, let's grow
      if (numEntries * ALLOWED_LOAD_FACTOR >= numSlotsUsed) {
        growAndRehash();
      } else {
        // Otherwise just rehash to clear REMOVED entries and don't grow
        rehash();
      }
    }
    // Here we may later consider implementing Brent's variation described on page 532
    int index = findForAdd(key);
    long keyIndex = keys[index];
    if (keyIndex != key) {
      keys[index] = key;
      numEntries++;
      if (keyIndex == NULL) {
        numSlotsUsed++;
      }
      return true;
    }
    return false;
  }
  
  public long[] toArray() {
    long[] result = new long[numEntries];
    for (int i = 0, position = 0; i < result.length; i++) {
      while ((keys[position] == NULL) || (keys[position] == REMOVED)) {
        position++;
      }
      result[i] = keys[position++];
    }
    return result;
  }
  
  public boolean remove(long key) {
    if ((key == NULL) || (key == REMOVED)) {
      return false;
    }
    int index = find(key);
    if (keys[index] == NULL) {
      return false;
    } else {
      keys[index] = REMOVED;
      numEntries--;
      return true;
    }
  }
  
  public boolean addAll(long[] c) {
    boolean changed = false;
    for (long k : c) {
      if (add(k)) {
        changed = true;
      }
    }
    return changed;
  }
  
  public boolean addAll(FastIDSet c) {
    boolean changed = false;
    for (long k : c.keys) {
      if ((k != NULL) && (k != REMOVED) && add(k)) {
        changed = true;
      }
    }
    return changed;
  }
  
  public boolean removeAll(long[] c) {
    boolean changed = false;
    for (long o : c) {
      if (remove(o)) {
        changed = true;
      }
    }
    return changed;
  }
  
  public boolean removeAll(FastIDSet c) {
    boolean changed = false;
    for (long k : c.keys) {
      if ((k != NULL) && (k != REMOVED) && remove(k)) {
        changed = true;
      }
    }
    return changed;
  }
  
  public boolean retainAll(FastIDSet c) {
    boolean changed = false;
    for (int i = 0; i < keys.length; i++) {
      long k = keys[i];
      if ((k != NULL) && (k != REMOVED) && !c.contains(k)) {
        keys[i] = REMOVED;
        numEntries--;
        changed = true;
      }
    }
    return changed;
  }
  
  public void clear() {
    numEntries = 0;
    numSlotsUsed = 0;
    Arrays.fill(keys, NULL);
  }
  
  private void growAndRehash() {
    if (keys.length * ALLOWED_LOAD_FACTOR >= MAX_INT_SMALLER_TWIN_PRIME) {
      throw new IllegalStateException("Can't grow any more");
    }
    rehash(nextTwinPrime((int) (ALLOWED_LOAD_FACTOR * keys.length)));
  }
  
  public void rehash() {
    rehash(nextTwinPrime((int) (ALLOWED_LOAD_FACTOR * numEntries)));
  }
  
  private void rehash(int newHashSize) {
    long[] oldKeys = keys;
    numEntries = 0;
    numSlotsUsed = 0;
    keys = new long[newHashSize];
    Arrays.fill(keys, NULL);
    int length = oldKeys.length;
    for (int i = 0; i < length; i++) {
      long key = oldKeys[i];
      if ((key != NULL) && (key != REMOVED)) {
        add(key);
      }
    }
  }
  
  /**
   * Convenience method to quickly compute just the size of the intersection with another .
   * 
   * @param other
   *           to intersect with
   * @return number of elements in intersection
   */
  public int intersectionSize(FastIDSet other) {
    int count = 0;
    for (long key : other.keys) {
      if ((key != NULL) && (key != REMOVED) && (keys[find(key)] != NULL)) {
        count++;
      }
    }
    return count;
  }
  
  @Override
  public FastIDSet clone() {
    FastIDSet clone;
    try {
      clone = (FastIDSet) super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new AssertionError();
    }
    clone.keys = keys.clone();
    return clone;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    long[] keys = this.keys;
    int max = keys.length;
    for (int i = 0; i < max; i++) {
      long key = keys[i];
      if (key != NULL && key != REMOVED) {
        hash = 31 * hash + ((int) (key >> 32) ^ (int) key);
      }
    }
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FastIDSet)) {
      return false;
    }
    FastIDSet otherMap = (FastIDSet) other;
    long[] otherKeys = otherMap.keys;
    int length = keys.length;
    int otherLength = otherKeys.length;
    int max = Math.min(length, otherLength);

    int i = 0;
    while (i < max) {
      long key = keys[i];
      long otherKey = otherKeys[i];
      if (key == NULL || key == REMOVED) {
        if (otherKey != NULL && otherKey != REMOVED) {
          return false;
        }
      } else {
        if (key != otherKey) {
          return false;
        }
      }
      i++;
    }
    while (i < length) {
      long key = keys[i];
      if (key != NULL && key != REMOVED) {
        return false;
      }
      i++;
    }
    while (i < otherLength) {
      long key = otherKeys[i];
      if (key != NULL && key != REMOVED) {
        return false;
      }
      i++;
    }
    return true;
  }
  
  @Override
  public String toString() {
    if (isEmpty()) {
      return "[]";
    }
    StringBuilder result = new StringBuilder();
    result.append('[');
    for (long key : keys) {
      if ((key != NULL) && (key != REMOVED)) {
        result.append(key).append(',');
      }
    }
    result.setCharAt(result.length() - 1, ']');
    return result.toString();
  }
  
}
