package blog.utopian.nerd;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

// Note: Add '--add-exports java.base/sun.nio.ch=ALL-UNNAMED' in VM options to run these programs on
// JDK 17 and above.
public class ProjectOne {

  public static void main(String[] args) {

    // Step 1: Create SparkSession.
    SparkSession sparkSession =
        SparkSession.builder().appName("CSV-To-File").master("local").getOrCreate();

    // Dataset<Row> is known as Dataframe in Spark.

    // Step 2: Read the data from source csv.
    Dataset<Row> dataset =
        sparkSession
            .read()
            .format("csv")
            .option("header", true)
            .load("src/main/resources/name-comments.txt");

    // Step 3: Do transformation on dataset.
    dataset =
        dataset
            .filter(dataset.col("comment").rlike("\\d+"))
            .withColumn(
                "full_name", concat(dataset.col("first_name"), lit(" "), dataset.col("last_name")));

    // Step 4: Print the transformed data to console.
    dataset.show();

    // Step 5: Write the transformed data to into output csv file.
    dataset
        .write()
        .format("csv")
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .save(
            "/Users/anshulgautam/Projects/Codebase/apache-spark-with-java-course-exercises/apache-spark-with-java-course-exercises/output");

    // Step 6: Close the spark session.
    sparkSession.close();
  }
}
