package blog.utopian.nerd;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Note: Add '--add-exports java.base/sun.nio.ch=ALL-UNNAMED' in VM options to run these programs on
// JDK 17 and above.
public class ProjectOne {

  public static void main(String[] args) {

    SparkSession sparkSession =
        SparkSession.builder().appName("CSV-To-DB").master("local").getOrCreate();

    Dataset<Row> dataset =
        sparkSession
            .read()
            .format("csv")
            .option("header", true)
            .load("src/main/resources/name-comments.txt");

    dataset =
        dataset
            .filter(dataset.col("comment").rlike("\\d+"))
            .withColumn(
                "full_name", concat(dataset.col("first_name"), lit(" "), dataset.col("last_name")));

    dataset.show();
  }
}
