package thefeed;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.nativelibs4java.opencl.CLBuildException;
import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLIntBuffer;
import com.nativelibs4java.opencl.CLKernel;
import com.nativelibs4java.opencl.CLLongBuffer;
import com.nativelibs4java.opencl.CLMem;
import com.nativelibs4java.opencl.CLProgram;
import com.nativelibs4java.opencl.CLQueue;
import com.nativelibs4java.opencl.JavaCL;
import com.sun.tools.javac.util.Pair;
import thefeed.mahout.FastIDSet;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Calibrate a platform to see how quickly it can scan entries. Run with -server and 2G of memory.
 * <p/>
 * User: sam
 * Date: 5/22/11
 * Time: 2:00 PM
 */
public class OpenCL {

  private static final int FOLLOWEES = 1000;

  public static void main(String[] args) throws ExecutionException, InterruptedException, CLBuildException, IOException {
    CLContext context = JavaCL.createBestContext();
    StringBuffer sb = new StringBuffer();
    Files.copy(new File("src/main/resources/feedscan.cl"), Charsets.UTF_8, sb);
    String myKernelSource = sb.toString();
    CLProgram program = context.createProgram(myKernelSource).build();
    CLKernel kernel = program.createKernel("myKernel");
    CLQueue queue = context.createDefaultQueue();
    ByteOrder order = context.getByteOrder();

    Random r = new Random();
    FastIDSet comparisons = new FastIDSet(10000);
    for (int i = 0; i < FOLLOWEES; i++) {
      comparisons.add((long) r.nextInt(RANGE));
    }
    long[] set = comparisons.getKeys();

    int length = set.length;
    CLLongBuffer clSetBuffer = createCLBuffer(context, order, length);
    LongBuffer setbuffer = clSetBuffer.map(queue, CLMem.MapFlags.Write);
    for (int i = 0; i < set.length; i++) {
      setbuffer.put(i, set[i]);
    }
    clSetBuffer.unmap(queue, setbuffer);

    LinkedFeed head = null;
    LinkedFeed current = null;
    for (int j = 0; j < BLOCKS; j++) {
      LinkedFeed tmp = current;
      CLLongBuffer longBuffer = createCLBuffer(context, order, TIMES / BLOCKS * 2);
      LongBuffer feed = longBuffer.map(queue, CLMem.MapFlags.Write);
      current = new LinkedFeed(longBuffer, current);
      current.next = tmp;
      head = current;
      for (int i = 0; i < TIMES / BLOCKS * 2; i += 2) {
        feed.put(i, r.nextInt(RANGE));
        feed.put(i + 1, 0);
      }
      longBuffer.unmap(queue, feed);
    }

    while(true) {
      test(context, kernel, queue, length, clSetBuffer, head);
    }
  }

  private static void test(CLContext context, CLKernel kernel, CLQueue queue, int length, CLLongBuffer clSetBuffer, LinkedFeed head) {
    LinkedFeed current;
    long start = System.currentTimeMillis();
    long hits = 0;
    System.out.println("TOTAL,HITS");
    List<Pair<CLEvent, CLIntBuffer>> pairs = new ArrayList<Pair<CLEvent, CLIntBuffer>>();
    for (current = head; current != null; current = current.next) {
      CLLongBuffer clFeed = current.value;
      CLIntBuffer clhits = context.createIntBuffer(CLMem.Usage.Output, 1);
      CLEvent kernelCompletion;
      // The same kernel can be safely used by different threads, as long as setArgs + enqueueNDRange are in a synchronized block
      synchronized (kernel) {
        // setArgs will throw an exception at runtime if the types / sizes of the arguments are incorrect
        kernel.setArgs(clFeed, clSetBuffer, clhits, length);
        // Ask for 1-dimensional execution of length dataSize, with auto choice of local workgroup size :
        kernelCompletion = kernel.enqueueNDRange(queue, new int[]{TIMES/BLOCKS});
      }
      pairs.add(new Pair<CLEvent, CLIntBuffer>(kernelCompletion, clhits));
    }
    for (Pair<CLEvent, CLIntBuffer> pair : pairs) {
      hits += pair.snd.read(queue, 0, 1, pair.fst).get(0);
    }
    long result = TIMES / (System.currentTimeMillis() - start);
    System.out.println(result + "," + hits);
  }

  private static CLLongBuffer createCLBuffer(CLContext context, ByteOrder order, int length) {
    return context.createLongBuffer(CLMem.Usage.Input, ByteBuffer.allocateDirect(length * 8).order(order).asLongBuffer(), false);
  }

  private static int RANGE = 100000;
  private static int BLOCKS = 4;
  private static int TIMES = 30000000;

  /**
   * We create the feed by linking together reverse chronological epochs of entries.
   * <p/>
   * User: sam
   * Date: 5/22/11
   * Time: 2:01 PM
   */
  private static class LinkedFeed {
    CLLongBuffer value;
    LinkedFeed next;

    public LinkedFeed(CLLongBuffer value, LinkedFeed next) {
      this.value = value;
      this.next = next;
    }
  }
}
