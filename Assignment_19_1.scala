package Assignment_19

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Assignment_19_1 extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("example")
    .config("spark.sql.warehouse.dir","C://ACADGILD")
    .getOrCreate()

  // A CSV dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
  val dataset_1 = spark.sqlContext.read.format("csv").option("header", "true")
    .option("inferSchema", true)
    .load("C:/ACADGILD/Big Data/SESSION_19/Sports_data.txt").toDF()


  // Register this DataFrame as a table.
  dataset_1.createOrReplaceTempView("sports")

  //--------------------------------------------------------------------------------------------------------------------
  //------------------------------------------ PROBLEM 1 ---------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------------------

  val prob_1 = spark.sql("select year, count(*) as no_of_gold_medals  from sports " +
                            "where medal_type='gold' group by year ").show()

  //--------------------------------------------------------------------------------------------------------------------
  //------------------------------------------ PROBLEM 2 ---------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------------------

  println("Number of silver medals  won by USA in each sport are : ")
  val prob_2 = spark.sql("select sports,count(*) from sports where country = 'USA' and medal_type = 'silver' " +
                          "group by sports")
                            .show()
}
