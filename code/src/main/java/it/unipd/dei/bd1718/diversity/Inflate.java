package it.unipd.dei.bd1718.diversity;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.stream.LongStream;

public class Inflate {

  private static Logger logger = LoggerFactory.getLogger(Inflate.class);

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--output", required = true, description = "Path to the output vector dataset")
    String output;

    @Parameter(names = "--distinct", required = true, description = "The number of distinct elements")
    int distinct;

    @Parameter(names = "--replicated", required = true, description = "The number of replicas of one of the distinct elements")
    int replicated;

    @Parameter(names = "--deviation", description = "The standard deviation of the gaussian noise")
    double deviation = 0.0;

  }

  public static void main(String[] args) throws IOException {
    System.out.println("Reading command line options");
    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    SparkConf conf = new SparkConf(true)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setAppName("Preprocess");
    JavaSparkContext sc = new JavaSparkContext(conf);
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    // Verify command line arguments
    if (fs.exists(new Path(arguments.output))) {
      logger.error("output already exists, not overwriting");
      return;
    }

    JavaRDD<Tuple2<Long, Vector>> vectors = InputOutput.readVectorsPairs(sc, arguments.input);
    ArrayList<Tuple2<Long, Vector>> sample = new ArrayList<>();
    sample.addAll(vectors.takeSample(false, arguments.distinct));
    Tuple2<Long, Vector> replicant = new Tuple2<>(-1L, sample.get(0)._2());
    sample.add(replicant);

    int replicas = arguments.replicated;
    double deviation = arguments.deviation;

    JavaRDD<Tuple2<Long, Vector>> replicated = sc.parallelize(sample).flatMap((t) -> {
      if (t._1() >= 0) {
        return Collections.singleton(t).iterator();
      } else {
        Vector v = t._2();
        Random rnd = new Random();
        return LongStream.rangeClosed(1, replicas)
                .mapToObj((l) -> {
                  long id = -l;
                  if (deviation != 0.0) {
                    double[] data = new double[v.size()];
                    for (int i=0; i<v.size(); i++) {
                      double x = v.apply(i);
                      double noise = rnd.nextGaussian()*deviation;
                      data[i] = x + x*noise;
                    }
                    return new Tuple2<>(id, Vectors.dense(data));
                  } else {
                    return new Tuple2<>(id, v);
                  }
                }).iterator();
      }
    }).repartition(vectors.getNumPartitions()).cache();

    long cnt = replicated.count();
    logger.info("Dataset with replicas has {} elements", cnt);

    InputOutput.writeVectorsPairs(JavaPairRDD.fromJavaRDD(replicated), arguments.output);
  }

}
