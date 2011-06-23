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

import thefeed.FollowSet;

import java.util.Arrays;

/**
 * @see FastByIDMap
 */
public final class FastIDSet2 implements FollowSet {

  private static final double ALLOWED_LOAD_FACTOR = 1.5;

  /** Dummy object used to represent a key that has been removed. */
  private static final long REMOVED = Long.MAX_VALUE;
  private static final long NULL = Long.MIN_VALUE;
  private static final double LOG2 = Math.log(2);

  private long[] keys;
  private int numEntries;
  private int numSlotsUsed;
  private int mask;

  public FastIDSet2(int size) {
    int hashSize = nextPowerOfTwo((int) (ALLOWED_LOAD_FACTOR * size));
    keys = new long[hashSize];
    mask = hashSize - 1;
    Arrays.fill(keys, NULL);
  }

  @Override
  public void report() {
  }

  public static final int MAX_INT_SMALLER_TWIN_PRIME = 2147482949;

  public static int nextPowerOfTwo(int n) {
    if (n > MAX_INT_SMALLER_TWIN_PRIME) {
      throw new IllegalArgumentException();
    }
    double v = Math.log(n) / LOG2;
    return (int) (Math.pow(2, (int) v + 1));
  }

  /**
   * @see #findForAdd(long)
   */
  private int find(long key) {
    long[] keys = this.keys;
    int mask = this.mask;
    int index = (int) key & mask;
    long currentKey = keys[index];
    while ((key != currentKey) && (currentKey != NULL)) { // note: true when currentKey == REMOVED
      index = (index + 1) & mask;
      currentKey = keys[index];
    }
    return index;
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
    int index = find(key);
    long found = keys[index];
    if (found != key) {
      // assert found == NULL
      keys[index] = key;
      numEntries++;
      if (found == NULL) {
        numSlotsUsed++;
      }
      return true;
    }
    return false;
  }

  private void growAndRehash() {
    if (keys.length * ALLOWED_LOAD_FACTOR >= MAX_INT_SMALLER_TWIN_PRIME) {
      throw new IllegalStateException("Can't grow any more");
    }
    rehash(nextPowerOfTwo((int) (ALLOWED_LOAD_FACTOR * keys.length)));
  }
  
  public void rehash() {
    rehash(nextPowerOfTwo((int) (ALLOWED_LOAD_FACTOR * numEntries)));
  }
  
  private void rehash(int newHashSize) {
    long[] oldKeys = keys;
    numEntries = 0;
    numSlotsUsed = 0;
    keys = new long[newHashSize];
    mask = newHashSize - 1;
    Arrays.fill(keys, NULL);
    int length = oldKeys.length;
    for (int i = 0; i < length; i++) {
      long key = oldKeys[i];
      if ((key != NULL) && (key != REMOVED)) {
        add(key);
      }
    }
  }
}
