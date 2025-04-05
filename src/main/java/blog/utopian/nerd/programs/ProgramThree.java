package blog.utopian.nerd.programs;

import static blog.utopian.nerd.util.SparkSessionUtil.getSparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * In this project, we have tried to explore union of two different datasets and output the
 * resultant dataset into the console.
 */
public class ProgramThree {

  private static final Logger LOGGER = LogManager.getLogger(ProgramThree.class);

  public static void main(String[] args) {

    SparkSession sparkSession = getSparkSession("Union-Parks-Data");

    Dataset<Row> durhamDataset = getDurhamDataset(sparkSession);
    // LOGGER.info("durhamDataset created is below:");
    // durhamDataset.show(10);

    Dataset<Row> philadelphiaDataset = getPhiladelphiaDataset(sparkSession);
    // LOGGER.info("philadelphiaDataset created is below:");
    // philadelphiaDataset.show(10);

    // Making union based on name. Unlike sql, order of columns here doesnt matter.
    Dataset<Row> unionDataset = unionDatasets(durhamDataset, philadelphiaDataset);
    unionDataset.show(200, 10);

    System.out.println("We have total " + unionDataset.count() + " records in the dataset");
    System.out.println("Number of partitions " + unionDataset.rdd().getPartitions().length);

    sparkSession.close();
  }

  private static Dataset<Row> unionDatasets(
      Dataset<Row> durhamDataset, Dataset<Row> philadelphiaDataset) {
    return durhamDataset.unionByName(philadelphiaDataset);
  }

  private static Dataset<Row> getDurhamDataset(SparkSession sparkSession) {

    Dataset<Row> durhamDataset =
        sparkSession
            .read()
            .format("json")
            .option("multiline", true)
            .load("src/main/resources/durham-parks.json");

    durhamDataset =
        durhamDataset
            .withColumn(
                "park_id",
                concat(
                    durhamDataset.col("datasetid"),
                    lit("_"),
                    durhamDataset.col("fields.objectid"),
                    lit("_Durham")))
            .withColumn("park_name", durhamDataset.col("fields.park_name"))
            .withColumn("city", lit("Durham"))
            .withColumn("address", durhamDataset.col("fields.address"))
            .withColumn("has_playground", durhamDataset.col("fields.playground"))
            .withColumn("zipcode", durhamDataset.col("fields.zip"))
            .withColumn("lands_in_acres", durhamDataset.col("fields.acres"))
            .withColumn("geoX", durhamDataset.col("geometry.coordinates").getItem(0))
            .withColumn("geoY", durhamDataset.col("geometry.coordinates").getItem(1))
            .drop("fields")
            .drop("geometry")
            .drop("record_timestamp")
            .drop("recordid")
            .drop("datasetid");

    return durhamDataset;
  }

  private static Dataset<Row> getPhiladelphiaDataset(SparkSession sparkSession) {

    Dataset<Row> philadelphiaDataset =
        sparkSession
            .read()
            .format("csv")
            .option("header", true)
            .option("multiline", true)
            .load("src/main/resources/philadelphia-recreations.csv");

    philadelphiaDataset =
        philadelphiaDataset
            // .filter(lower(philadelphiaDataset.col("USE_")).like("%park%"))
            .filter("lower(USE_) like '%park%'")
            .withColumn("park_id", concat(lit("phil_"), philadelphiaDataset.col("OBJECTID")))
            .withColumnRenamed("ASSET_NAME", "park_name")
            .withColumn("city", lit("Philadelphia"))
            .withColumnRenamed("ADDRESS", "address")
            .withColumn("has_playground", lit("UNKNOWN"))
            .withColumnRenamed("ZIPCODE", "zipcode")
            .withColumnRenamed("ACREAGE", "lands_in_acres")
            .withColumn("geoX", lit("UNKNOWN"))
            .withColumn("geoY", lit("UNKNOWN"))
            .drop("SITE_NAME")
            .drop("OBJECTID")
            .drop("CHILD_OF")
            .drop("TYPE")
            .drop("USE_")
            .drop("DESCRIPTION")
            .drop("SQ_FEET")
            .drop("ALLIAS")
            .drop("CHRONOLOGY")
            .drop("NOTES")
            .drop("DATE_EDITED")
            .drop("EDITED_BY")
            .drop("OCCUPANT")
            .drop("TENANT")
            .drop("LABEL");

    return philadelphiaDataset;
  }
}
