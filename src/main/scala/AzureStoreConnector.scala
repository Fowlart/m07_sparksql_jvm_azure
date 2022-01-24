package main.scala

import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.file.datalake.{DataLakeDirectoryClient, DataLakeFileSystemClient, DataLakeServiceClient, DataLakeServiceClientBuilder}

import java.util.Date

object AzureStoreConnector {

  def getDataLakeServiceClient(accountName: String, accountKey: String): DataLakeServiceClient = {
    val sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey)
    val builder = new DataLakeServiceClientBuilder
    builder.credential(sharedKeyCredential)
    builder.endpoint("https://" + accountName + ".dfs.core.windows.net")
    builder.buildClient
  }


  def deleteCreateFileSystem(serviceClient: DataLakeServiceClient, name: String): DataLakeFileSystemClient
  = {
    val date = new Date()
    serviceClient.createFileSystem(s"$name${date.getTime}")
  }

  def createDirectory(serviceClient: DataLakeServiceClient, fileSystemName: String, directoryName: String): DataLakeDirectoryClient = {
    val fileSystemClient = serviceClient.getFileSystemClient(fileSystemName)
    val directoryClient = fileSystemClient.createDirectory(directoryName)
    directoryClient
  }

  def uploadFileBulk(fileSystemClient: DataLakeFileSystemClient, directoryClient: DataLakeDirectoryClient,
                     filenameInAzure: String,
                     filePathLocal: String): Unit = {
    val fileClient = directoryClient.getFileClient(filenameInAzure)
    fileClient.uploadFromFile(filePathLocal)
  }
}
