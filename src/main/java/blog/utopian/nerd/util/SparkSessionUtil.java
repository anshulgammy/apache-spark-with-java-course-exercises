package blog.utopian.nerd.util;

import org.apache.spark.sql.SparkSession;

public final class SparkSessionUtil {

  private SparkSessionUtil() {}

  public static SparkSession getSparkSession(String appName) {
    return SparkSession.builder().appName(appName).master("local").getOrCreate();
  }
}
