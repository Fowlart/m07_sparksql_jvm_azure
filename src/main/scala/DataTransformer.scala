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
}
