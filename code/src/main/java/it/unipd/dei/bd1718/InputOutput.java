package it.unipd.dei.bd1718;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import scala.Tuple2;

/**
 * Static methods to read and write datasets in (compressed) json
 * format. These functions internally use an API that we have not seen
 * in class, which are wrapped for your convenience.
 */
public class InputOutput {

  /**
   * Read a dataset from the given path, using the given
   * JavaSparkContext. Returns a dataset of WikiPage objects.
   */
  public static JavaRDD<WikiPage> read(JavaSparkContext sc, String path) {
    return new SparkSession(sc.sc())
            .read()
            .json(path)
            .as(WikiPage.getEncoder())
            .javaRDD();
  }

  /**
   * Writes the given dataset to the given path. If the path already
   * exists, then the invocation fails.
   *
   * Note: this function outputs a directory containing several files
   * instead of a single file like the one you downloaded from the
   * Project's web page. This is fine, as Spark can deal transparently
   * with directories containing datasets split in several files. In
   * particular, a dataset written with this write function can be
   * loaded using the read function defined above
   */
  public static void write(JavaRDD<WikiPage> rdd, String path) {
    new SparkSession(rdd.context())
            .createDataset(rdd.rdd(), WikiPage.getEncoder())
            .write()
            .option("compression", "bzip2")
            .json(path);
  }

  public static JavaRDD<Tuple2<Long, Vector>> readVectorsPairs(JavaSparkContext sc, String path) {
    return new SparkSession(sc.sc())
            .read()
            .parquet(path)
            .as(Encoders.tuple(Encoders.LONG(), Encoders.kryo(Vector.class)))
            .javaRDD();
  }

  public static void writeVectorsPairs(JavaPairRDD<Long, Vector> vectors, String path) {
    new SparkSession(vectors.context())
            .createDataset(vectors.rdd(), Encoders.tuple(Encoders.LONG(), Encoders.kryo(Vector.class)))
            .write()
            .option("compression", "gzip")
            .parquet(path);
  }

  public static JavaRDD<Vector> readVectors(JavaSparkContext sc, String path) {
    return new SparkSession(sc.sc())
            .read()
            .parquet(path)
            .as(Encoders.kryo(Vector.class))
            .javaRDD();
  }

  public static void writeVectors(JavaRDD<Vector> vectors, String path) {
    new SparkSession(vectors.context())
            .createDataset(vectors.rdd(), Encoders.kryo(Vector.class))
            .write()
            .option("compression", "gzip")
            .parquet(path);
  }
}