__kernel void myKernel(__global const long* feed, __global const long* keys, __global int* output, int mask) {
   int i = get_global_id(0);
   long key = feed[i*2];
   int index = (int) key & mask;
   long currentKey = keys[index];
   while (key != currentKey && currentKey != LONG_MIN) {
     index = (index + 1) & mask;
     currentKey = keys[index];
   }
   if (currentKey == key) {
     atom_inc(output);
   }
}