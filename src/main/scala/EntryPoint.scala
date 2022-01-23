package main.scala

import org.apache.spark.sql.SparkSession

import java.io.FileInputStream
import java.util.Properties

object EntryPoint extends App {

  val sparkSession = SparkSession.builder()
    .appName("my_executor")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext
  val prop = new Properties()
  prop.load(new FileInputStream("src/main/resources/creds.properties"))

  sparkSession.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
  sparkSession.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  sparkSession.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", prop.getProperty("client.id"))
  sparkSession.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", prop.getProperty("client.secret"))
  sparkSession.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

  val hotel_weather = sparkSession
    .read
    .parquet("abfs://m07sparksql@bd201stacc.dfs.core.windows.net/hotel-weather")

  hotel_weather.show()

  val expedia = sparkSession
    .read
    .format("avro")
    .load("abfs://m07sparksql@bd201stacc.dfs.core.windows.net/expedia")

  expedia.show()
}
