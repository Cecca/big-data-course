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

}
