__kernel void myKernel(__global const long* feed, __global const long* keys, __global int* output, int hashSize) {
   int i = get_global_id(0);
   long key = feed[i*2];
   int mask = hashSize - 1;
   int index = (int) key & mask;
   long currentKey = keys[index];
   while (currentKey != LONG_MIN && currentKey != LONG_MAX && key != currentKey) {
     index = (index + 1) & mask;
     currentKey = keys[index];
   }
   if (currentKey == key) {
     atom_inc(output);
   }
}