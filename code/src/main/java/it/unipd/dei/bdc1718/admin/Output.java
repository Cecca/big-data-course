package it.unipd.dei.bdc1718.admin;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.io.*;

public class Output {

  public static void writeVectors(JavaRDD<Vector> vectors, String path) {
    vectors.map(Output::vectorToStr)
            .saveAsTextFile(path, BZip2Codec.class);
  }

  public static void writeVectorsSeq(JavaRDD<Vector> vectors, String path) {
    try (FileOutputStream fos = new FileOutputStream(path);
         OutputStream os = new BufferedOutputStream(fos);
         PrintWriter pw = new PrintWriter(os)) {
      vectors.map(Output::vectorToStr)
              .toLocalIterator()
              .forEachRemaining(pw::println);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String vectorToStr(Vector v) {
    StringBuffer sb = new StringBuffer();
    double[] data = v.toArray();
    for (int i=0; i<data.length-1; i++) {
      sb.append(data[i]).append(' ');
    }
    sb.append(data[data.length-1]);
    return sb.toString();
  }


}
