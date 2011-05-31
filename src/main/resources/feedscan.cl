__kernel void myKernel(__global const long* feed, __global const long* keys, __global int* output, int hashSize) {
   int i = get_global_id(0);
   long key = feed[i*2];
   int theHashCode = (int) key & 0x7FFFFFFF;
   int jump = 0;
   int index = theHashCode % hashSize;
   long currentKey = keys[index];
   while (currentKey != LONG_MIN && currentKey != LONG_MAX && key != currentKey) {
     if (jump == 0) jump = 1 + theHashCode % (hashSize - 2);
     if (index < jump) {
       index += hashSize - jump;
     } else {
       index -= jump;
     }
     currentKey = keys[index];
   }
   if (currentKey == key) {
     atom_inc(output);
   }
}