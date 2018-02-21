package it.unipd.dei.bd1718;

import org.apache.spark.SparkConf;

public class Utils {

  public static int getNumCores(SparkConf conf) {
    int executorCores = conf.getInt("spark.executor.cores", -1);
    int numExecutors = conf.getInt("spark.executor.instances", -1);
    if (executorCores < 0 || numExecutors < 0) {
      return -1;
    }
    return executorCores * numExecutors;
  }

  /**
   * String representing the memory needed for a n x n matrix
   */
  public static String matrixMemory(int n) {
    final long numBytes = n*n*8;
    final long KB = 1024;
    final long MB = 1024*KB;
    final long GB = 1024*MB;
    if (numBytes >= GB) {
      return (numBytes / ((double) GB)) + " GB";
    } else if (numBytes >= MB) {
      return (numBytes / ((double) MB)) + " MB";
    } else if (numBytes >= KB) {
      return (numBytes / ((double) KB)) + " KB";
    } else {
      return numBytes + " bytes";
    }
  }

}
