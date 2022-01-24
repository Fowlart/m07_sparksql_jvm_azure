package main.scala

import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}

import java.io.{File, FileInputStream}
import java.util.Properties

object EntryPoint extends App {


  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

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
  val expediaLocalPath = "src/main/resources/expedia"
  val hotel_weatherLocalPath = "src/main/resources/hotel-weather"

  try {
    val cashed_hotel_weather = sparkSession.read.parquet(hotel_weatherLocalPath)
    val cashed_expedia = sparkSession.read.format("avro").load(expediaLocalPath)
    println(s"${cashed_hotel_weather.count()} - count of lines for 'hotel_weather'")
    println(s"${cashed_expedia.count()} - count of lines for 'expedia'")
    println("files was retrieved from local hash...")
    val expediaInputFileName = getListOfFiles(expediaLocalPath).filter(file => file.getName.endsWith(".avro")).head.getName
    val hotel_weatherInputFileName = getListOfFiles(hotel_weatherLocalPath).filter(file => file.getName.endsWith(".parquet")).head.getName

    // create container in Azure Data Lake Storage Gen2, and save data there
    val dataLakeServiceClient = AzureStoreConnector.getDataLakeServiceClient(prop.getProperty("azure.storage.accountName"), prop.getProperty("azure.storage.accountKey"))
    val fileSystem = AzureStoreConnector.deleteCreateFileSystem(dataLakeServiceClient, "db-work-container")
    val dataLakeDirectoryClient = AzureStoreConnector.createDirectory(dataLakeServiceClient, fileSystem.getFileSystemName, "input")
    AzureStoreConnector.uploadFileBulk(fileSystem, dataLakeDirectoryClient, "expedia.avro", s"$expediaLocalPath/$expediaInputFileName")
    AzureStoreConnector.uploadFileBulk(fileSystem, dataLakeDirectoryClient, "hotel_weather.parquet", s"$hotel_weatherLocalPath/$hotel_weatherInputFileName")

    //transformations



  }
  catch {
        // if the files does not exists locally -> retrieve them first
    case ex: AnalysisException =>
      println(ex)
      val hotel_weather = sparkSession
        .read
        .parquet("abfs://m07sparksql@bd201stacc.dfs.core.windows.net/hotel-weather")

      val expedia = sparkSession
        .read
        .format("avro")
        .load("abfs://m07sparksql@bd201stacc.dfs.core.windows.net/expedia")

      expedia
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(expediaLocalPath)

      hotel_weather
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(hotel_weatherLocalPath)

      println("Data were cashed locally. Please rerun the app.")
  }
}
