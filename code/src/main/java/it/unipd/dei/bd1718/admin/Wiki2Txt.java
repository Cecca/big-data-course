package it.unipd.dei.bd1718.admin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import it.unipd.dei.bd1718.InputOutput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Wiki2Txt {

  private static Logger logger = LoggerFactory.getLogger(Sample.class);

  private static class Args {

    @Parameter(names = "--input", required = true, description = "Path to the input dataset")
    String input;

    @Parameter(names = "--output", required = true, description = "Path to the output vector dataset")
    String output;

  }

  public static void main(String[] args) throws IOException {
    Args arguments = new Args();
    JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);

    SparkConf conf = new SparkConf(true)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setAppName("Simple preprocessing");
    JavaSparkContext sc = new JavaSparkContext(conf);
    logger.info(sc.getConf().toDebugString());
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

    // Verify command line arguments
    if (fs.exists(new Path(arguments.output))) {
      logger.error("output already exists, not overwriting");
      return;
    }

    JavaRDD<WikiPage> pages = WikiPage.readPages(sc, arguments.input);
    pages.map((page) -> page.getText().replace('\n', ' '))
            .saveAsTextFile(arguments.output);
  }

}
