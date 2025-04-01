package blog.utopian.nerd;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ProjectOne {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("CSV-To-DB").master("local").getOrCreate();

        Dataset<Row> dataset = sparkSession.read().format("csv").option("header", true).load("src/main/resources/name-comments.txt");

        dataset.show();
    }
}
