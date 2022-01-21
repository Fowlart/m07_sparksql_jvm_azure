package main.scala

import org.apache.spark.sql.SparkSession

import java.io.FileInputStream
import java.util.Properties

object EntryPoint extends App {

  val sparkSession = SparkSession.builder()
    .appName("Executor for m06_sparkbasics_jvm_azure")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val prop = new Properties()
  prop.load(new FileInputStream("src/main/resources/creds.properties"))
  sparkSession.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
  sparkSession.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  sparkSession.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", s"${prop.getProperty("client.id")}")
  sparkSession.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", s"${prop.getProperty("client.secret")}")
  sparkSession.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

  // weather
  val weatherDf = sparkSession.
    read.parquet("abfs://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather")

  // hotels
  val hotelsDf = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("abfs://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels")

  weatherDf.show()
}
