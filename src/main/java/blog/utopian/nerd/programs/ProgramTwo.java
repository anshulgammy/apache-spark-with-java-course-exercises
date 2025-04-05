package blog.utopian.nerd.programs;

import static blog.utopian.nerd.util.SparkSessionUtil.getSparkSession;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * In this project, we have tried exploring how to load single line and multiline json,csv,xml
 * files, and do some basic transformation, and then output the resulting dataframe. We have also
 * explored on how to create custom schema, and try to find out how it differs from inferSchema
 * which spark creates on its own.
 */
public class ProgramTwo {

  public static void main(String[] args) {

    // When you're first learning Spark, it's a good idea to keep all of the logging turned on so
    // that you can follow what Spark is doing during every step of running your program. But if you
    // get tired of seeing all of that being printed to the console, you can turn off INFO log
    // entries by following the steps below.
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    // inferCSVSchema();

    // customCSVSchema();

    // jsonSingleLinesParser();

    jsonMultiLinesParser();
  }

  private static void inferCSVSchema() {

    // Step 1: Create SparkSession.
    SparkSession sparkSession = getSparkSession("Complex-CSV-To-File");

    // Step 2: Read the data from source csv.
    Dataset<Row> dataset =
        sparkSession
            .read()
            .format("csv")
            .option("header", true)
            .option(
                "multiline",
                true) // if values are spread across more than one line, then read them as well
            // under the column.
            .option("sep", ";") // define what is the separator being used in the file.
            .option(
                "quote", "^") // define what character should be used in place of quote in the file.
            .option(
                "dateFormat",
                "M/d/yy") // define the dateFormat to be used while reading value for the column.
            .option(
                "inferSchema",
                true) // telling  spark to infer the schema and information on its datatype on its
            // own. We can also provide a custom schema for spark to read the file.
            .load("src/main/resources/amazon-products.txt");

    // Step 3: Output the data read from the file and stored in the dataset to the console.
    // Shows 5 rows and for each column in the row, it will show max 50 characters. After 50,
    // characters will be truncated.
    dataset.show(5, 50);

    // Printing the schema, just to display what schema spark had created on its own. Remember we
    // had used .option("inferSchema", true).
    dataset.printSchema();

    sparkSession.close();
  }

  private static void customCSVSchema() {

    // Step 1: Create SparkSession.
    SparkSession sparkSession = getSparkSession("Complex-CSV-To-File");

    // Step 2: Create custom schema using StructType.
    StructType structType =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("product_id", DataTypes.IntegerType, true),
              DataTypes.createStructField("item_name", DataTypes.StringType, false),
              DataTypes.createStructField("published_on", DataTypes.DateType, false),
              DataTypes.createStructField("url", DataTypes.StringType, false)
            });

    // Step 3: Read the data from source csv.
    Dataset<Row> dataset =
        sparkSession
            .read()
            .format("csv")
            .option("header", true)
            .option(
                "multiline",
                true) // if values are spread across more than one line, then read them as well
            // under the column.
            .option("sep", ";") // define what is the separator being used in the file.
            .option(
                "quote", "^") // define what character should be used in place of quote in the file.
            .option(
                "dateFormat",
                "M/d/yy") // define the dateFormat to be used while reading value for the column.
            .schema(structType) // asking spark to use our custom schema while reading from the csv
            // file.
            // own. We can also provide a custom schema for spark to read the file.
            .load("src/main/resources/amazon-products.txt");

    // Step 4: Output the data read from the file and stored in the dataset to the console.
    // Shows 5 rows and for each column in the row, it will show max 50 characters. After 50,
    // characters will be truncated.
    dataset.show(5, 50);

    // Printing the schema, just to display what our custom schema looks like.
    dataset.printSchema();

    // Step 5: Close the spark session.
    sparkSession.close();
  }

  public static void jsonSingleLinesParser() {

    // Step 1: Create SparkSession.
    SparkSession sparkSession = getSparkSession("Read-Json-Single-Lines");

    // Step 2: Read the data from the json file.
    // In this json file, every line is a complete json in its own.
    Dataset<Row> dataset =
        sparkSession.read().format("json").load("src/main/resources/single-lines.json");

    // Step 3: Output the data read from the file and stored in the dataset to the console.
    dataset.show(5, 50);

    // Printing the schema, just to display what our custom schema looks like.
    dataset.printSchema();

    // Step 4: Close the spark session.
    sparkSession.close();
  }

  public static void jsonMultiLinesParser() {

    // Step 1: Create SparkSession.
    SparkSession sparkSession = getSparkSession("Read-Json-Multi-Lines");

    // Step 2: Read the data from the json file.
    // In this json file, every complete json entry spans over multiple lines in the file.
    Dataset<Row> dataset =
        sparkSession
            .read()
            .format("json")
            .option("multiline", true)
            .load("src/main/resources/multi-lines.json");

    // Step 3: Output the data read from the file and stored in the dataset to the console.
    dataset.show(5, 50);

    // Printing the schema, just to display what our custom schema looks like.
    dataset.printSchema();

    // Step 4: Close the spark session.
    sparkSession.close();
  }
}
