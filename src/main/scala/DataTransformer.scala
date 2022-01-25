package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataTransformer extends App {

  val sparkSession = SparkSession.builder()
    .appName("Data Transformer app")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val cashed_hotel_weather = sparkSession.read.parquet(ConstantHelper.EXPEDIA_LOCALPATH)
  val cashed_expedia = sparkSession.read.format("avro").load(ConstantHelper.HOTEL_WEATHER_LOCALPATH)

  val joining_condition = ((cashed_expedia.col("hotel_id") === cashed_hotel_weather.col("id"))
    && ((cashed_hotel_weather.col("wthr_date") === cashed_expedia.col("srch_ci"))
    || (cashed_hotel_weather.col("wthr_date") === cashed_expedia.col("srch_co"))))

  val joinedDF = cashed_expedia.join(cashed_hotel_weather, joining_condition, "inner")

  //Top 10 hotels with max absolute temperature difference by month
  joinedDF
    .groupBy(col("hotel_id"), col("month"))
    .agg(max(col("avg_tmpr_c")).as("max_temp"), min(col("avg_tmpr_c")).as("min_temp"))
    .withColumn("absolute_temperature_difference", col("max_temp") - col("min_temp"))
    .sort(col("absolute_temperature_difference").desc)

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

  // For visits with extended stay (more than 7 days) calculate weather trend (the day temperature
  // difference between last and first day of stay) and average temperature during stay
  val subResultsWithAvgTemp = joinedDF
    .withColumn("stay_days", datediff(col("srch_co"), col("srch_ci")))
    .filter(col("stay_days") > 7)
    .withColumn("wthr_date", to_date(col("wthr_date")))
    .groupBy(col("user_id"), col("hotel_id"))
    .agg(
      avg("avg_tmpr_c").as("average_temperature"),
      max("wthr_date").as("leave_weather_date"),
      min("wthr_date").as("checkin_weather_date")
    )

  val joinedCond2_0 = subResultsWithAvgTemp.col("hotel_id") === cashed_hotel_weather.col("id")
  val joinedCond2_1 = subResultsWithAvgTemp.col("leave_weather_date") === cashed_hotel_weather.col("wthr_date")
  val joinedCond2_2 = subResultsWithAvgTemp.col("checkin_weather_date") === cashed_hotel_weather.col("wthr_date")

  val tempView = subResultsWithAvgTemp.join(cashed_hotel_weather, joinedCond2_0 && (joinedCond2_1 || joinedCond2_2), "inner")
    .select("user_id", "hotel_id", "average_temperature", "leave_weather_date", "checkin_weather_date", "wthr_date", "avg_tmpr_c")
    .orderBy("user_id", "wthr_date")

  val res3 = tempView.groupBy("user_id","hotel_id")
    .agg(avg("average_temperature").as("average_temperature"),
    (max("avg_tmpr_c")-min("avg_tmpr_c")).as("absolute_temp_difference"))
    .orderBy(col("absolute_temp_difference").desc)
    .limit(10)
}
