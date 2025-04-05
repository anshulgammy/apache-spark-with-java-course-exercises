package blog.utopian.nerd.programs;

import static blog.utopian.nerd.util.SparkSessionUtil.getConvertedDate;
import static blog.utopian.nerd.util.SparkSessionUtil.getSparkSession;

import blog.utopian.nerd.model.House;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProgramFive {

  public static void main(String[] args) {

    SparkSession sparkSession = getSparkSession("Custom-Dataset");

    Dataset<Row> dataframe =
        sparkSession
            .read()
            .format("csv")
            .option("inferSchema", true)
            .option("header", true)
            .option("sep", ";")
            .load("src/main/resources/houses.csv");

    Dataset<House> dataset = dataframe.map(new Mapper(), Encoders.bean(House.class));

    dataset.printSchema();
    dataset.show();
  }

  static class Mapper implements MapFunction<Row, House>, Serializable {

    @Override
    public House call(Row row) throws Exception {
      return new House(
          row.getAs("id"),
          row.getAs("address"),
          row.getAs("sqft"),
          row.getAs("price"),
          getConvertedDate(row.getAs("vacantBy").toString()));
    }
  }
}
