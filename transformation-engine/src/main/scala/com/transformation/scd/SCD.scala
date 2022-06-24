package com.transformation.scd

import java.io.{File, FileNotFoundException, IOException, OutputStream, PrintStream}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.{Files, Paths}
import com.google.gson.{Gson, JsonArray, JsonObject, JsonParser}
import com.microsoft.azure.datalake.store.ADLStoreClient
import com.microsoft.azure.datalake.store.oauth2.{AccessTokenProvider, ClientCredsTokenProvider}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession, types}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.joda.time.format.DateTimeFormat
import scala.annotation.tailrec
import com.microsoft.azure.datalake.store.{ADLStoreClient, DirectoryEntry, IfExists}
import com.microsoft.azure.datalake.store.oauth2.{AccessTokenProvider, ClientCredsTokenProvider}
import scala.reflect.io.Directory

object SCD {

  val log: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    println("Start")

    val flagExists = args(0).toBoolean
    val headerExists = args(1).toBoolean
    val primaryKey = args(2)//"id"
    val sourceSchemaName = args(3)
    val sourceSchemaString = URLtoSchemaString(sourceSchemaName) // "id:int,name:string,age:int,address:string,flag:string"
    val sourceLocation = args(4)
    val targetDBName = args(5)
    val targetTableName = args(6)
    val destinationLocation = args(7)
    val scdProcess = args(8)//"f","d","i"
    val valueSeparator = args(9)//"|"
    val flagColumn = args(10)//"flag"
    val insertFlag = args(11)//"i"
    val updateFlag = args(12)//"u"
    val deleteFlag = args(13)//"d"

    println("*********************************************************************************************")
    println("*********************************************************************************************")
    println("*********************************************************************************************")
    println("flagExists = " + flagExists)
    println("headerExists = " + headerExists)
    println("primaryKey = " + primaryKey)
    println("sourceSchemaName = " + sourceSchemaName)
    println("sourceSchemaString = " + sourceSchemaString)
    println("sourceLocation = " + sourceLocation)
    println("targetDBName = " + targetDBName)
    println("targetTableName = " + targetTableName)
    println("destinationLocation = " + destinationLocation)
    println("scdProcess = " + scdProcess)
    println("valueSeparator = " + valueSeparator)
    println("flag = " + flagColumn)
    println("insertFlag = " + insertFlag)
    println("updateFlag = " + updateFlag)
    println("deleteFlag = " + deleteFlag)
    println("*********************************************************************************************")
    println("*********************************************************************************************")
    println("*********************************************************************************************")

    val startDateColumnName = "start_date__"
    val endDateColumnName = "end_date__"
    val endDate = "9999-12-31"
    val currentDate = DateTimeFormat.forPattern("yyyy-MM-dd").print(System.currentTimeMillis())
    val currentYear = currentDate.split("-")(0)
    val currentMonth = currentDate.split("-")(1)
    val currentDay = currentDate.split("-")(2)

    if (sourceSchemaString == null) {
      println(s"Schema not found for ${sourceSchemaName} in schema registry. Exiting...")
      return ;
    }

    val spark = setupSpark()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    //removes flag from schema string and adds start date and end date columns
    val targetSchemaString = sourceSchemaString.toLowerCase().split(",", -1).filter(!_.startsWith(s"${flagColumn}:")).mkString(",") + s",${startDateColumnName}:string,${endDateColumnName}:string"

    // TESTING
    /*val sampleDf = Seq(
      ("1", "Sarah Tancredi", "29", "California", "2020-01-10", "9999-12-31"),
      ("3", "Fernando Sucre", "28", "Texas", "2020-01-10", "9999-12-31"),
      ("5", "Lincoln Burrows", "40", "Texas", "2020-01-10", "9999-12-31"),
      ("4", "Alexander Mahone", "35", "California", "2020-01-10", "9999-12-31"),
      ("2", "Michael Scofield", "31", "Illinois", "2020-01-10", "9999-12-31")).toDF("id", "name", "age", "address", "start_date__", "end_date__")
    val targetDF = sampleDf.where(col(endDateColumnName) === endDate)*/

    //transforms schema string to StructType
    val sourceSchema: StructType = StructType(sourceSchemaString.split(",").map(x => StructField(x.split(":")(0), dataTypeParser(x.split(":")(1)), true)))
    val targetSchema: StructType = StructType(targetSchemaString.split(",").map(x => StructField(x.split(":")(0), dataTypeParser(x.split(":")(1)), true)))

    // CHECK IF SOURCE PATH EXISTS
    if (sourceLocation.startsWith("adl://")) {
      val client: ADLStoreClient = adlsClientCreator()
      if (!client.checkExists(sourceLocation.replaceFirst("adl://azuredatalakestore.net", ""))) {
        println(s"Source Path(ADLS): ${sourceLocation} is inexistent. Exiting...");
        return ;
      }
    } else {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (!fs.exists(new Path(s"${sourceLocation}"))) {
        println(s"Source Path(HDFS): ${sourceLocation} is inexistent. Exiting...");
        return ;
      }
    }
    if(scdProcess == "d") {
      // READING TARGET(SCD) TABLE
      val targetDF = if (spark.catalog.tableExists(targetDBName, targetTableName)) {
        spark.table(s"${targetDBName}.${targetTableName}").where(col(endDateColumnName) === endDate)
      } else {
        val targetTableSchema = targetSchemaString.replaceAll(":", " ").replaceAll("long", "BIGINT").replaceAll(s",${endDateColumnName} string", "")
        spark.sql("CREATE EXTERNAL TABLE " + targetDBName + "." + targetTableName + "(" + targetTableSchema + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + valueSeparator + "' PARTITIONED BY (" + endDateColumnName + " String) STORED AS TEXTFILE LOCATION '" + destinationLocation + "'")
        spark.sql("ALTER TABLE " + targetDBName + "." + targetTableName + " ADD PARTITION(" + endDateColumnName + "='" + endDate + "')")
        spark.table(s"${targetDBName}.${targetTableName}").where(col(endDateColumnName) === endDate)
      }
      println("Existing Table")
      targetDF.show

      // READING CURRENT DATA
      val sourceDF = if (headerExists) {
        spark.read.option("header", true).schema(sourceSchema).option("delimiter", valueSeparator).csv(sourceLocation)
      } else {
        spark.read.schema(sourceSchema).option("delimiter", valueSeparator).csv(sourceLocation)
      }
      println("New Current Table")
      sourceDF.show

      val sourceColumnsWithoutFlag = sourceDF.columns.filter(!_.equalsIgnoreCase(flagColumn))
      val renamedSourceDF = getDfWithRenamedColumns(sourceDF, sourceDF.columns, "attach")
      val joinedDF = targetDF.join(renamedSourceDF, targetDF(primaryKey) === renamedSourceDF(primaryKey + "__"), "full_outer")

      val underscoredColumns = sourceDF.columns.map(colName => colName + "__")
      val underscoredColumnsWithoutFlag = sourceDF.columns.map(colName => colName + "__").filter(!_.equalsIgnoreCase(flagColumn + "__"))

      // CHECK IF DATA CONTAINS COLUMN THAT SPECIFIES I,U,D FLAG
      val finalDF = if (flagExists) {
        println("Flag Exists")

        val deletedRecordsIdDF = sourceDF.filter(col(flagColumn) === deleteFlag).select(col(primaryKey).as(primaryKey + "__"))
        val deletedRecords = targetDF.join(deletedRecordsIdDF, targetDF(primaryKey) === deletedRecordsIdDF(primaryKey + "__"), "inner")
          .drop(primaryKey + "__")
          .drop(endDateColumnName)
          .withColumn(endDateColumnName, lit(currentDate))
        //      println("Deleted Records")
        //      deletedRecords.show()

        val asIsRecords = joinedDF.filter(col(primaryKey + "__").isNull).select(targetDF.columns.head, targetDF.columns.tail: _*)
        //      println("AsIs Records")
        //      asIsRecords.show()

        // UNCHANGED RECORDS AND REMOVE DELETED RECORDS
        val unchangedRecords = getUnchangedRecords(joinedDF.filter(col(primaryKey) === col(primaryKey + "__")), sourceColumnsWithoutFlag)
          .filter(col(flagColumn + "__") =!= deleteFlag)
          .select(targetDF.columns.head, targetDF.columns.tail: _*)
        //      println("unchanged Records")
        //      unchangedRecords.show()

        val insertedRecords = getDfWithRenamedColumns(
          joinedDF.filter(col(primaryKey).isNull).select(underscoredColumns.head, underscoredColumns.tail: _*),
          sourceDF.columns, "detach")
          .drop(flagColumn)
          .withColumn(startDateColumnName, lit(currentDate))
          .withColumn(endDateColumnName, lit(endDate))
        //      println("Inserted Records")
        //      insertedRecords.show()

        val updatedRecords = getUpdatedRecords(joinedDF.filter(col(primaryKey) === col(primaryKey + "__")), sourceColumnsWithoutFlag)
        //      println("Updated Records")
        //      updatedRecords.show()

        val updatedRecordsToDelete = updatedRecords.select(targetDF.columns.head, targetDF.columns.tail: _*)
          .drop("end_date_")
          .withColumn(endDateColumnName, lit(currentDate))
        //      println("updatedRecordsToDelete")
        //      updatedRecordsToDelete.show

        val updatedRecordsToInsert = getDfWithRenamedColumns(updatedRecords
          .select(underscoredColumnsWithoutFlag.head, underscoredColumnsWithoutFlag.tail: _*), sourceColumnsWithoutFlag, "detach")
          .drop("start_date_")
          .withColumn(startDateColumnName, lit(currentDate))
          .withColumn(endDateColumnName, lit(endDate))
        //      println("updatedRecordsToInsert")
        //      updatedRecordsToInsert.show

        asIsRecords.union(unchangedRecords).union(insertedRecords).union(updatedRecordsToInsert).union(updatedRecordsToDelete).union(deletedRecords)

      } else {
        println("Flag Does Not Exists")

        val renamedSourceDF = getDfWithRenamedColumns(sourceDF, sourceDF.columns, "attach")
        val joinedDF = targetDF.join(renamedSourceDF, targetDF(primaryKey) === renamedSourceDF(primaryKey + "__"), "full_outer")

        val underscoredColumns = sourceDF.columns.map(colName => colName + "__")

        val asIsRecords = joinedDF.filter(col(primaryKey + "__").isNull).select(targetDF.columns.head, targetDF.columns.tail: _*)
        //      println("AsIs Records")
        //      asIsRecords.show()

        val unchangedRecords = getUnchangedRecords(joinedDF.filter(col(primaryKey) === col(primaryKey + "__")), sourceColumnsWithoutFlag)
          .select(targetDF.columns.head, targetDF.columns.tail: _*)
        //      println("unchanged Records")
        //      unchangedRecords.show()

        val insertedRecords = getDfWithRenamedColumns(
          joinedDF.filter(col(primaryKey).isNull).select(underscoredColumns.head, underscoredColumns.tail: _*),
          sourceDF.columns, "detach")
          .drop(flagColumn)
          .withColumn(startDateColumnName, lit(currentDate))
          .withColumn(endDateColumnName, lit(endDate))
        //      println("Inserted Records")
        //      insertedRecords.show()

        val updatedRecords = getUpdatedRecords(joinedDF.filter(col(primaryKey) === col(primaryKey + "__")), sourceColumnsWithoutFlag)
        //      println("Updated Records")
        //      updatedRecords.show()

        val updatedRecordsToDelete = updatedRecords.select(targetDF.columns.head, targetDF.columns.tail: _*)
          .drop("end_date_")
          .withColumn(endDateColumnName, lit(currentDate))
        //      println("updatedRecordsToDelete")
        //      updatedRecordsToDelete.show

        val updatedRecordsToInsert = getDfWithRenamedColumns(updatedRecords
          .select(underscoredColumnsWithoutFlag.head, underscoredColumnsWithoutFlag.tail: _*), sourceColumnsWithoutFlag, "detach")
          .drop("start_date_")
          .withColumn(startDateColumnName, lit(currentDate))
          .withColumn(endDateColumnName, lit(endDate))
        //      println("updatedRecordsToInsert")
        //      updatedRecordsToInsert.show

        asIsRecords.union(unchangedRecords).union(insertedRecords).union(updatedRecordsToInsert).union(updatedRecordsToDelete)
      }
      finalDF.show

      finalDF.write.parquet(s"/tmp/${targetTableName}")
      val t = spark.read.parquet(s"/tmp/${targetTableName}")

      spark.sql("ALTER TABLE " + targetDBName + "." + targetTableName + " ADD IF NOT EXISTS PARTITION(" + endDateColumnName + " ='" + currentDate + "')")
      t.filter(col(endDateColumnName) === currentDate).write.mode(SaveMode.Overwrite).option("timestampFormat","yyyy-MM-dd hh:mm:ss.SSSSSSS").option("delimiter", valueSeparator).csv(destinationLocation + "/" + endDateColumnName + "=" + currentDate)
      t.filter(col(endDateColumnName) === endDate).write.mode(SaveMode.Overwrite).option("timestampFormat","yyyy-MM-dd hh:mm:ss.SSSSSSS").option("delimiter", valueSeparator).csv(destinationLocation + "/" + endDateColumnName + "=" + endDate)

      // CLEANUP
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(s"/tmp/${targetTableName}"))) {
        fs.delete(new Path(s"/tmp/${targetTableName}"), true)
      }
      moveSourceProcessedData(fs,sourceLocation)
    }
    else if(scdProcess == "f"){
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      copySourceToDestination(fs,sourceLocation,destinationLocation,true)
      moveSourceProcessedData(fs,sourceLocation)
      val sourceTableSchema = sourceSchemaString.replaceAll(":", " ").replaceAll("long", "BIGINT")
      if(headerExists) {
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + targetDBName + "." + targetTableName + "(" + sourceTableSchema + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + valueSeparator + "' STORED AS TEXTFILE LOCATION '" + destinationLocation + "' TBLPROPERTIES ('skip.header.line.count' = '1')")
      }
      else{
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + targetDBName + "." + targetTableName + "(" + sourceTableSchema + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + valueSeparator + "' STORED AS TEXTFILE LOCATION '" + destinationLocation + "'")
      }
    }
    else if(scdProcess == "i"){
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val destinationLocationWithDate = destinationLocation + "/" + currentYear + "/" + currentMonth + "/" + currentDay
      copySourceToDestination(fs,sourceLocation,destinationLocationWithDate,false)
      moveSourceProcessedData(fs,sourceLocation)
      val sourceTableSchema = sourceSchemaString.replaceAll(":", " ").replaceAll("long", "BIGINT")
      if(headerExists) {
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + targetDBName + "." + targetTableName + "(" + sourceTableSchema + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + valueSeparator + "' STORED AS TEXTFILE LOCATION '" + destinationLocation + "' TBLPROPERTIES ('skip.header.line.count' = '1')")
      }
      else{
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + targetDBName + "." + targetTableName + "(" + sourceTableSchema + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + valueSeparator + "' STORED AS TEXTFILE LOCATION '" + destinationLocation + "'")
      }
    }

    println("End")
  }

  def getUpdatedRecords(df: DataFrame,
                        columns: Array[String]): DataFrame = {
    val filterCondition = columns.map(columnName => {
      !(df(columnName).eqNullSafe(df(columnName + "__")))
    })
    df.filter(filterCondition.reduce(_ || _))
  }

  def getUnchangedRecords(df: DataFrame,
                          columns: Array[String]): DataFrame = {
    val filterCondition = columns.map(columnName => {
      df(columnName).eqNullSafe(df(columnName + "__"))
    })
    df.filter(filterCondition.reduce(_ && _))
  }

  def applyFilters(df: DataFrame,
                   columns: Array[String],
                   table: String,
                   bool: Boolean): DataFrame = {
    if (bool) {

      val filterCondition = columns.map(columnName => {
        df(columnName).eqNullSafe(df(table + "_" + columnName))
      })
      df.filter(filterCondition.reduce(_ && _))

    } else {

      val filterCondition = columns.map(columnName => {
        !(df(columnName).eqNullSafe(df(table + "_" + columnName)))
      })
      df.filter(filterCondition.reduce(_ || _))

    }
  }

  @tailrec
  final def getDfWithRenamedColumns(df: DataFrame,
                                    columns: Array[String],
                                    keyword: String): DataFrame = {
    if (columns.isEmpty) {
      df
    } else {
      keyword.toLowerCase() match {
        case "attach" =>
          getDfWithRenamedColumns(
            df.withColumnRenamed(columns.head, columns.head + "__"),
            columns.tail,
            keyword)
        case "detach" =>
          getDfWithRenamedColumns(
            df.withColumnRenamed(columns.head + "__", columns.head),
            columns.tail,
            keyword)
      }
    }
  }

  //parse datatypes to DataTypes(StructType)
  def dataTypeParser(datatypeName: String): DataType = {
    datatypeName.toLowerCase() match {
      case "string"    => StringType
      case "int"       => IntegerType
      case "long"      => LongType
      case "boolean"   => BooleanType
      case "double"    => DoubleType
      case "float"     => FloatType
      case "date"      => DateType
      case "timestamp" => TimestampType
      case _           => throw new Exception("Unexpected Datatype Encountered")
    }
  }

  def URLtoSchemaString(sourceSchemaName: String): String = {
    import sys.process._
    val curlResponse = List("curl", "-XGET", "--negotiate", "-u:nifi", "http://nifiserver.com:7788/api/v1/schemaregistry/schemas/" + sourceSchemaName + "/versions/latest", "-k").!!
    log.info(curlResponse)
    val parser = new JsonParser
    val jsonObject = parser.parse(curlResponse.toString).getAsJsonObject()
    if (jsonObject.get("schemaText") == null) {
      return null;
    }
    val avroSchema = parser.parse(jsonObject.get("schemaText").getAsString()).getAsJsonObject
    val schemaStr = new StringBuilder()

    val fieldItr = avroSchema.get("fields").getAsJsonArray.iterator()
    while (fieldItr.hasNext) {
      val field = fieldItr.next().getAsJsonObject
      if (schemaStr.size != 0)
        schemaStr.append(",")
      if (field.get("type").isInstanceOf[JsonObject]){
        schemaStr.append(field.get("name").getAsString + ":" + field.get("type").getAsJsonObject.get("logicalType").getAsString)
      }
      else if (field.get("type").isInstanceOf[JsonArray]){
        val dataType = if(field.get("type").getAsJsonArray.get(0).getAsString.toLowerCase.equals("null"))
          field.get("type").getAsJsonArray.get(1).getAsString
        else
          field.get("type").getAsJsonArray.get(0).getAsString
        schemaStr.append(field.get("name").getAsString + ":" + dataType)
      }
      else {
        schemaStr.append(field.get("name").getAsString + ":" + field.get("type").getAsString)
      }
    }
    return schemaStr.toString.replace("timestamp-millis","timestamp")
  }

  def setupSpark(): SparkSession = {
    val conf: SparkConf = new SparkConf()
      .set("hive.exec.compress.output", "false")
      .set("mapred.output.compress", "false")
      .set("hive.mapred.supports.subdirectories", "true")
      .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    SparkSession
      .builder
      //.master("local[*]")
      .enableHiveSupport()
      .config(conf)
      .appName("SCD")
      .getOrCreate()
  }

  def moveSourceProcessedData(fs:FileSystem, sourceLocation:String): Unit = {
    if(sourceLocation.startsWith("adl://")) {
      val sourceADLSLocation = sourceLocation.replaceFirst("adl://azuredatalakestore.net", "")
      val client: ADLStoreClient = adlsClientCreator()
      if (!client.checkExists(s"${sourceADLSLocation}" + "/" + "processed")) {
        client.createDirectory(s"${sourceADLSLocation}" + "/" + "processed")
      }
      val directoryFiles = client.enumerateDirectory(s"${sourceADLSLocation}")
      directoryFiles.toArray().foreach(filename => {
        val fileName:String =  filename.asInstanceOf[DirectoryEntry].name
        if(fileName != "processed") {
          val input = client.getReadStream(s"${sourceADLSLocation}" + "/" + fileName)
          val outputStream: OutputStream = client.createFile(s"${sourceADLSLocation}" + "/" + "processed" + "/" + fileName, IfExists.OVERWRITE)
          val out = new PrintStream(outputStream)
          scala.collection.Iterator.continually(input.read).takeWhile(-1 !=).foreach(out.write)
          input.close
          client.delete(s"${sourceADLSLocation}" + "/" + fileName)
          out.close
          outputStream.close
        }
      });
    }
    else{
      if (fs.exists(new Path(s"${sourceLocation}" + "/" + "processed"))) {
        fs.moveFromLocalFile(new Path(s"${sourceLocation}"), new Path(s"${sourceLocation}" + "/" + "processed"))
      }
      else {
        fs.mkdirs(new Path(s"${sourceLocation}" + "/" + "processed"))
        fs.moveFromLocalFile(new Path(s"${sourceLocation}"), new Path(s"${sourceLocation}" + "/" + "processed"))
      }
    }
  }

  def copySourceToDestination(fs:FileSystem,sourceLocation:String,destinationLocation:String,overwrite:Boolean): Unit ={
    if(sourceLocation.startsWith("adl://")) {
      val sourceADLSLocation = sourceLocation.replaceFirst("adl://azuredatalakestore.net", "")
      val destinationADLSLocation = destinationLocation.replaceFirst("adl://azuredatalakestore.net", "")
      val client: ADLStoreClient = adlsClientCreator()
      if (!client.checkExists(s"${destinationADLSLocation}")){
        client.createDirectory(s"${destinationADLSLocation}")
      }
      if(overwrite){
        deleteAllFiles(client,destinationLocation)
      }
      val directoryFiles = client.enumerateDirectory(s"${sourceADLSLocation}")
      directoryFiles.toArray().foreach(filename => {
        val fileName = filename.asInstanceOf[DirectoryEntry].name
        if(fileName != "processed") {
          val input = client.getReadStream(s"${sourceADLSLocation}" + "/" + fileName)
          val outputStream: OutputStream = client.createFile(s"${destinationADLSLocation}" + "/" + fileName, IfExists.OVERWRITE)
          val out = new PrintStream(outputStream)
          scala.collection.Iterator.continually(input.read).takeWhile(-1 !=).foreach(out.write)
          input.close
          out.close
          outputStream.close
        }
      });
    }
    else{
      if (!fs.exists(new Path(s"${destinationLocation}"))) {
        fs.mkdirs(new Path(s"${destinationLocation}"))
      }
      fs.moveFromLocalFile(new Path(s"${sourceLocation}"), new Path(s"${destinationLocation}"))
    }
  }

  def adlsClientCreator(): ADLStoreClient ={
    val clientId = ""
    val clientKey = ""
    val authTokenEndpoint = ""
    val accountName = ""
    val provider: AccessTokenProvider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey)
    val client: ADLStoreClient = ADLStoreClient.createClient(accountName, provider)
    return client
  }

  def deleteAllFiles(client: ADLStoreClient, destinationLocation: String): Unit ={
    val destinationADLSLocation = destinationLocation.replaceFirst("adl://azuredatalakestore.net", "")
    val directoryFiles = client.enumerateDirectory(s"${destinationADLSLocation}")
    directoryFiles.toArray().foreach(filename => {
      val fileName = filename.asInstanceOf[DirectoryEntry].name
      client.delete(s"${destinationADLSLocation}" + "/" +fileName)
    });
  }

}
