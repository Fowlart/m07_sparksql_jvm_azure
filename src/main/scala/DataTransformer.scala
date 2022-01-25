package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataTransformer extends App {

  val sparkSession = SparkSession.builder()
    .appName("Data Transformer app")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val expediaLocalPath = "src/main/resources/expedia"
  val hotel_weatherLocalPath = "src/main/resources/hotel-weather"

  val cashed_hotel_weather = sparkSession.read.parquet(hotel_weatherLocalPath)
  val cashed_expedia = sparkSession.read.format("avro").load(expediaLocalPath)

  val joining_condition = cashed_expedia.col("hotel_id") === cashed_hotel_weather.col("id")
  val joinedDF = cashed_expedia.join(cashed_hotel_weather, joining_condition, "inner")

  //Top 10 hotels with max absolute temperature difference by month
  joinedDF
    .groupBy(col("hotel_id"), col("month"))
    .agg(max(col("avg_tmpr_c")).as("max_temp"), min(col("avg_tmpr_c")).as("min_temp"))
    .withColumn("absolute_temperature_difference", col("max_temp") - col("min_temp"))
    .sort(col("absolute_temperature_difference").desc)
    .show(10)

  // Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months,
  // it should be counted for all affected months.
  cashed_expedia
    .withColumn("month", month(col("date_time")))
    .groupBy(col("hotel_id"), col("month"))
    .agg(
      sum("srch_adults_cnt")
        .as("adults_count_sum"),
      sum("srch_children_cnt")
        .as("children_count_sum"),
      sum("srch_rm_cnt")
        .as("rm_count_sum"))
    .withColumn("total_person_count",
      col("children_count_sum") + col("adults_count_sum") + col("rm_count_sum"))
    .orderBy(col("total_person_count").desc)
    .show()


}
