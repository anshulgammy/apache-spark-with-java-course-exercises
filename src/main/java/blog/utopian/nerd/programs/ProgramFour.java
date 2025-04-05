package blog.utopian.nerd.programs;

import static blog.utopian.nerd.util.SparkSessionUtil.getSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * In this Project, we have discussed on Dataframe to Dataset conversion. What is dataset and
 * dataframe(highly optimized by Tungsten engine). It is highly recommended that we convert any
 * dataset back to dataframe before doing any operations on it. If we use groupBy, any aggregation
 * etc dataset will be converted to dataframe.
 *
 * <p>Dataset<Row> is a dataframe, the underlying data structure of spark.
 *
 * <p>Dataset<T> where T is any other type, is a dataset.
 */
public class ProgramFour {

  public static void main(String[] args) {
    SparkSession sparkSession = getSparkSession("DS-To-DF");

    List<String> itemList = Arrays.asList("Banana", "Car", "Banana", "Computer", "Mobile", "Car");

    Dataset<String> dataset = sparkSession.createDataset(itemList, Encoders.STRING());

    // dataSetToDataFrameConversion(sparkSession, itemList);

    dataset = dataset.map(new Mapper(), Encoders.STRING());
    dataset.show();

    String reducedValue = dataset.reduce(new Reducer());
    System.out.println(reducedValue);

    sparkSession.close();
  }

  private static void dataSetToDataFrameConversion(
      SparkSession sparkSession, List<String> itemList) {

    Dataset<String> dataset = sparkSession.createDataset(itemList, Encoders.STRING());
    // Dataset<Row> df = dataset.toDF();
    Dataset<Row> dataframe = dataset.groupBy("value").count();
    // Dataset<String> ds = dataframe.as(Encoders.STRING());

    dataframe.show();
  }

  /** Mappers take one value and outputs other one value. */
  static class Mapper implements MapFunction<String, String>, Serializable {

    @Override
    public String call(String value) throws Exception {
      return "Processed " + value;
    }
  }

  /** Reducers take multiple values and outputs other one value. */
  static class Reducer implements ReduceFunction<String>, Serializable {

    @Override
    public String call(String one, String two) throws Exception {
      return one + two;
    }
  }
}
