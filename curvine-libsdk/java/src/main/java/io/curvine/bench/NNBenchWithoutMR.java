// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.curvine.bench;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This program executes a specified operation that applies load to 
 * the NameNode. Possible operations include create/writing files,
 * opening/reading files, renaming files, and deleting files.
 * 
 * When run simultaneously on multiple nodes, this program functions 
 * as a stress-test and benchmark for namenode, especially when 
 * the number of bytes written to each file is small.
 * 
 * This version does not use the map reduce framework
 * 
 */
public class NNBenchWithoutMR {
  
  private static final Log LOG = LogFactory.getLog(
                                            "org.apache.hadoop.hdfs.NNBench");
  
  // variable initialized from command line arguments
  private static long startTime = 0;
  private static int numFiles = 0;
  private static long threads = 1;
  private static long filesPerReport = 1;
  private static int bytesToWrite = 0; // default is 0
  private static short replicationFactorPerFile = 1; // default is 1
  private static AtomicInteger totalSuccess = new AtomicInteger(0);
  private static AtomicInteger totalException = new AtomicInteger(0);
  private static Path baseDir = null;
    
  // variables initialized in main()
  private static FileSystem fileSys = null;

  private static boolean useStaticPath = true;
  private static ThreadLocal<Integer> threadIndex = new ThreadLocal<>();
  private static ThreadLocal<Path> taskDir = ThreadLocal.withInitial(() -> {
    if (useStaticPath) {
      int idx = threadIndex.get();
      return new Path(baseDir, "taskDir_" + idx);
    }
    String randomString = RandomStringUtils.randomAlphabetic(10);
    return new Path(baseDir, randomString);
  });
  private static byte[] buffer;
  private static long maxExceptionsPerFile = 200;

  public static class IndexedThread extends Thread {
    int idx;
    Runnable op;

    public IndexedThread(Runnable op, int idx) {
      this.idx = idx;
      this.op = op;
    }

    @Override
    public void run() {
      threadIndex.set(idx);
      op.run();
    }
  }
  /**
   * Returns when the current number of seconds from the epoch equals
   * the command line argument given by <code>-startTime</code>.
   * This allows multiple instances of this program, running on clock
   * synchronized nodes, to start at roughly the same time.
   */

  static void barrier() {
    long sleepTime;
    while ((sleepTime = startTime - System.currentTimeMillis()) > 0) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException ex) {
        //This left empty on purpose
      }
    }
  }
    
  static private void handleException(String operation, Throwable e, 
                                      int singleFileExceptions) {
    LOG.warn("Exception while " + operation + ": " +
             StringUtils.stringifyException(e));
    if (singleFileExceptions >= maxExceptionsPerFile) {
      throw new RuntimeException(singleFileExceptions + 
        " exceptions for a single file exceeds threshold. Aborting");
    }
  }
  
  /**
   * Create and write to a given number of files.  Repeat each remote
   * operation until is succeeds (does not throw an exception).
   *
   * @return the number of exceptions caught
   */
  static int createWrite() {
    int totalExceptions = 0;
    FSDataOutputStream out = null;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // create file until is succeeds or max exceptions reached
        try {
          out = fileSys.create(
              new Path(taskDir.get(), "" + index));

          long toBeWritten = bytesToWrite;
          int nbytes = (int) Math.min(buffer.length, toBeWritten);
          out.write(buffer, 0, nbytes);
          out.close();
          totalSuccess.incrementAndGet();
          success = true;
        } catch (IOException ioe) { 
          success=false; 
          totalExceptions++;
          handleException("creating file #" + index, ioe,
                  ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
    
  /**
   * Open and read a given number of files.
   *
   * @return the number of exceptions caught
   */
  static int openRead() {
    int totalExceptions = 0;
    FSDataInputStream in;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      try {
        in = fileSys.open(new Path(taskDir.get(), "" + index), 512);
        long toBeRead = bytesToWrite;
        if (bytesToWrite > 0)  {
          while (toBeRead > 0) {
            int nbytes = (int) Math.min(buffer.length, toBeRead);
            toBeRead -= nbytes;
            try { // only try once && we don't care about a number of bytes read
              in.read(buffer, 0, nbytes);
              totalSuccess.incrementAndGet();
            } catch (IOException ioe) {
              totalExceptions++;
              handleException("reading from file #" + index, ioe,
                      ++singleFileExceptions);
            }
          }
        } else {
          totalSuccess.incrementAndGet();
        }

        in.close();
      } catch (IOException ioe) { 
        totalExceptions++;
        handleException("opening file #" + index, ioe, ++singleFileExceptions);
      }
    }
    return totalExceptions;
  }
    
  /**
   * Rename a given number of files.  Repeat each remote
   * operation until is succeeds (does not throw an exception).
   *
   * @return the number of exceptions caught
   */
  static int rename() {
    int totalExceptions = 0;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // rename file until is succeeds
        try {
          // Possible result of this operation is at no interest to us for it
          // can return false only if the namesystem
          // could rename the path from the name
          // space (e.g. no Exception has been thrown)
          fileSys.rename(new Path(taskDir.get(), "" + index),
              new Path(taskDir.get(), "A" + index));
          success = true;
          totalSuccess.incrementAndGet();
        } catch (IOException ioe) {
          success = false;
          totalExceptions++;
          handleException("creating file #" + index, ioe, ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
    
  /**
   * Delete a given number of files.  Repeat each remote
   * operation until is succeeds (does not throw an exception).
   *
   * @return the number of exceptions caught
   */
  static int delete() {
    int totalExceptions = 0;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // delete file until is succeeds
        try {
          // Possible result of this operation is at no interest to us for it
          // can return false only if namesystem
          // delete could remove the path from the name
          // space (e.g. no Exception has been thrown)
          fileSys.delete(new Path(taskDir.get(), "A" + index), true);
          success = true;
          totalSuccess.incrementAndGet();
        } catch (IOException ioe) {
          success=false; 
          totalExceptions++;
          handleException("creating file #" + index, ioe, ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }

  static Path getFinalPathByIndex(int index) {
    Path subdir0 = new Path(taskDir.get(), "" + index / 1000000 );
    Path subdir1 = new Path(subdir0, "" + (index%1000000)/1000);
    Path path = new Path(subdir1, "" + index % 1000);
    return path;
  }

  static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static long lastReportTime = System.currentTimeMillis();
  static LinkedList<Integer> history = new LinkedList();


  static synchronized void report() {
    while(true) {
      try {
        Thread.sleep(50L);
      } catch (InterruptedException var7) {
        return;
      }

      Date now = new Date();
      if (now.getTime() - lastReportTime > 1000L) {
        int lastTotalSuccess = 0;
        if (!history.isEmpty()) {
          lastTotalSuccess = history.getLast();
        }

        int thisTotal = totalSuccess.get();
        int qps = thisTotal - lastTotalSuccess;
        int avg10 = 0;
        int avg;
        if (history.size() > 10) {
          avg = (Integer)history.get(history.size() - 10);
          avg10 = (thisTotal - avg) / 10;
        }

        avg = thisTotal / (history.size() + 1);
        String time = df.format(now);
        System.out.println(time + " success:" + totalSuccess.get() + " exception:" + totalException.get() + ", qps:" + qps + ", avg10:" + avg10 + ", avg:" + avg);
        lastReportTime = now.getTime();
        history.offerLast(thisTotal);
      }
    }
  }

  static int mkdir() {
    int totalExceptions = 0;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // create file until is succeeds or max exceptions reached
        try {
          Path path = getFinalPathByIndex(index);
          success = fileSys.mkdirs(path);
          totalSuccess.incrementAndGet();
        } catch (IOException ioe) {
          success=false;
          totalExceptions++;
          totalException.incrementAndGet();
          handleException("creating file #" + index, ioe,
              ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
  static int rmdir() {
    int totalExceptions = 0;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // create file until is succeeds or max exceptions reached
        try {
          Path path = getFinalPathByIndex(index);
          success = fileSys.delete(path, false);
          totalSuccess.incrementAndGet();
        } catch (IOException ioe) {
          success=false;
          totalExceptions++;
          totalException.incrementAndGet();
          handleException("creating file #" + index, ioe,
              ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
  static int createEmpty() {
    int totalExceptions = 0;
    boolean success;
    for (int index = 0; index < numFiles; index++) {
      int singleFileExceptions = 0;
      do { // create file until is succeeds or max exceptions reached
        try {
          Path path = getFinalPathByIndex(index);
          FSDataOutputStream out = fileSys.create(path);
          out.close();
          success = true;
          totalSuccess.incrementAndGet();
        } catch (IOException ioe) {
          success=false;
          totalExceptions++;
          totalException.incrementAndGet();
          handleException("creating file #" + index, ioe,
              ++singleFileExceptions);
        }
      } while (!success);
    }
    return totalExceptions;
  }
  static int openEmpty() {
    int theIdx = threadIndex.get();
    taskDir.set(new Path(baseDir, "emptyfile-t" + theIdx));

    while (true) {
      boolean success;
      for (int index = 0; index < numFiles; index++) {
        int singleFileExceptions = 0;
        do { // create file until is succeeds or max exceptions reached
          try {
            Path path = getFinalPathByIndex(index);
            FSDataInputStream in = fileSys.open(path);
            in.close();
            success = true;
            totalSuccess.incrementAndGet();
          } catch (IOException ioe) {
            success=false;
            totalException.incrementAndGet();
            handleException("openEmpty file #" + index, ioe,
                ++singleFileExceptions);
          }
        } while (!success);
      }
    }
  }
  static int statusEmpty() {
    int theIdx = threadIndex.get();
    taskDir.set(new Path(baseDir, "emptyfile-t" + theIdx));

    while (true) {
      boolean success;
      for (int index = 0; index < numFiles; index++) {
        int singleFileExceptions = 0;
        do { // create file until is succeeds or max exceptions reached
          try {
            Path path = getFinalPathByIndex(index);
            FileStatus status = fileSys.getFileStatus(path);
            success = true;
            totalSuccess.incrementAndGet();
          } catch (IOException ioe) {
            success=false;
            totalException.incrementAndGet();
            handleException("statusEmpty file #" + index, ioe,
                ++singleFileExceptions);
          }
        } while (!success);
      }
    }
  }

  /**
   * This launches a given namenode operation (<code>-operation</code>),
   * starting at a given time (<code>-startTime</code>).  The files used
   * by the openRead, rename, and delete operations are the same files
   * created by the createWrite operation.  Typically, the program
   * would be run four times, once for each operation in this order:
   * createWrite, openRead, rename, delete.
   *
   * <pre>
   * Usage: nnbench 
   *          -operation <one of createWrite, openRead, rename, or delete>
   *          -baseDir <base output/input DFS path>
   *          -startTime <time to start, given in seconds from the epoch>
   *          -numFiles <number of files to create, read, rename, or delete>
   *          -bytesToWrite <number of blocks to create per file>
   *         [-bytesPerBlock <number of bytes to write to each block, default is 1>]
   *         [-bytesPerChecksum <value for io.bytes.per.checksum>]
   * </pre>
   *
   * @param args is an array of the program command line arguments
   * @throws IOException indicates a problem with test startup
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    String version = "NameNodeBenchmark.0.3";
    System.out.println(version);
    int bytesPerChecksum = -1;
    
    String usage =
      "Usage: nnbench " +
      "  -operation <one of createWrite, openRead, rename, or delete>\n " +
      "  -baseDir <base output/input DFS path>\n " +
      "  -startTime <time to start, given in seconds from the epoch>\n" +
      "  -numFiles <number of files to create>\n " +
      "  -replicationFactorPerFile <Replication factor for the files, default is 1>\n" +
      "  -bytesToWrite <number of blocks to create per file>\n" +
      "  [-bytesPerBlock <number of bytes to write to each block, default is 1>]\n" +
      "  [-bytesPerChecksum <value for io.bytes.per.checksum>]\n" +
      "Note: bytesPerBlock MUST be a multiple of bytesPerChecksum\n";
    
    String operation = null;
    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-baseDir")) {
        baseDir = new Path(args[++i]);
      } else if (args[i].equals("-numFiles")) {
        numFiles = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-threads")) {
        threads = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-bytesToWrite")) {
        bytesToWrite = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-filesPerReport")) {
        filesPerReport = Long.parseLong(args[++i]);
      } else if (args[i].equals("-bytesPerChecksum")) {
        bytesPerChecksum = Integer.parseInt(args[++i]);        
      } else if (args[i].equals("-replicationFactorPerFile")) {
        replicationFactorPerFile = Short.parseShort(args[++i]);
      } else if (args[i].equals("-startTime")) {
        startTime = Long.parseLong(args[++i]) * 1000;
      } else if (args[i].equals("-operation")) {
        operation = args[++i];
      } else if (args[i].equals("-confDir")) {
        File file = new File(Paths.get(args[++i], "curvine-site.xml").toString());
        conf.addResource(new Path(file.getPath()));
      } else {
        System.out.println(usage);
        System.exit(-1);
      }
    }

    
    System.out.println("Inputs: ");
    System.out.println("   operation: " + operation);
    System.out.println("   baseDir: " + baseDir);
    System.out.println("   startTime: " + startTime);
    System.out.println("   numFiles: " + numFiles);
    System.out.println("   threads: " + threads);
    System.out.println("   replicationFactorPerFile: " + replicationFactorPerFile);
    System.out.println("   bytesToWrite: " + bytesToWrite);
    System.out.println("   filesPerReport: " + filesPerReport);
    System.out.println("   bytesPerChecksum: " + bytesPerChecksum);
    
    if (operation == null ||  // verify args
        baseDir == null ||
        numFiles < 1)
      {
        System.err.println(usage);
        System.exit(-1);
      }

    fileSys = baseDir.getFileSystem(conf);

    fileSys.initialize(baseDir.toUri(), conf);

    String uniqueId = java.net.InetAddress.getLocalHost().getHostName();
    baseDir = new Path(baseDir, uniqueId);
    // initialize buffer used for writing/reading file
    buffer = new byte[(int) Math.min(bytesToWrite, 32768L)];

    List<Thread> threadList = new ArrayList<>();
    Runnable runnable = null;
    if (operation.equals("createWrite")) {
      if (!fileSys.mkdirs(baseDir)) {
        throw new IOException("Mkdirs failed to create " + taskDir.toString());
      }
      runnable = () -> createWrite();
    } else if (operation.equals("mkdir")) {
      runnable = () -> mkdir();
    } else if (operation.equals("rmdir")) {
      runnable = () -> rmdir();
    } else if (operation.equals("openRead")) {
      runnable = () -> openRead();
    } else if (operation.equals("rename")) {
      runnable = () -> rename();
    } else if (operation.equals("delete")) {
      runnable = () -> delete();
    } else if (operation.equals("createEmpty")) {
      useStaticPath = false;
      runnable = () -> createEmpty();
    } else if (operation.equals("loadEmpty")) {
      runnable = () -> createEmpty();
    } else if (operation.equals("openEmpty")) {
      runnable = () -> openEmpty();
    } else if (operation.equals("statusEmpty")) {
      runnable = () -> statusEmpty();
    } else {
      System.err.println(usage);
      System.exit(-1);
    }
    for (int i = 0; i< threads; i++) {
      IndexedThread t = new IndexedThread(runnable, i);
      threadList.add(t);
    }

    Thread reportThread = new Thread(() -> report());
    reportThread.setDaemon(true);
    reportThread.start();
    Date execTime;
    Date endTime;
    long duration;
    barrier(); // wait for coordinated start time
    execTime = new Date();
    System.out.println("Job started: " + startTime);
    for (Thread t : threadList) {
      t.start();
    }
    for (Thread t : threadList) {
      t.join();
    }
    endTime = new Date();
    System.out.println("Job ended: " + endTime);
    duration = (endTime.getTime() - execTime.getTime()) /1000;
    System.out.println("The " + operation + " job took " + duration + " seconds.");
    System.out.println("The job recorded " + totalException.get() + " exceptions.");
  }
}
