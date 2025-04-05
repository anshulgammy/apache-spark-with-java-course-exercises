package blog.utopian.nerd.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.spark.sql.SparkSession;

public final class SparkSessionUtil {

  public static final String DATE_FORMAT = "yyyy-mm-dd";

  private SparkSessionUtil() {}

  public static SparkSession getSparkSession(String appName) {
    return SparkSession.builder().appName(appName).master("local").getOrCreate();
  }

  public static Date getConvertedDate(String dateString) {

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);
    try {
      return simpleDateFormat.parse(dateString);

    } catch (ParseException e) {
      System.out.println("Error occurred while parsing the provided date " + dateString);
      e.printStackTrace();
    }
    return null;
  }
}
