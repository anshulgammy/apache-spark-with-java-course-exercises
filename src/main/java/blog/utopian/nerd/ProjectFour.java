package blog.utopian.nerd;

import static blog.utopian.nerd.util.SparkSessionUtil.getSparkSession;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProjectFour {

  public static void main(String[] args) {
    SparkSession sparkSession = getSparkSession("DS-To-DF");

    List<String> itemList = Arrays.asList("Banana", "Car", "Banana", "Computer", "Mobile", "Car");

    Dataset<String> dataset = sparkSession.createDataset(itemList, Encoders.STRING());
    // Dataset<Row> df = dataset.toDF();
    Dataset<Row> dataframe = dataset.groupBy("value").count();
    // Dataset<String> ds = dataframe.as(Encoders.STRING());

    dataframe.show();
  }
}
