package com.transformation.scd

import org.apache.spark.sql.SparkSession
import za.co.absa.spline.core.SparkLineageInitializer._
import org.apache.spark.sql.functions
import com.microsoft.azure.datalake.store.ADLStoreClient
import com.microsoft.azure.datalake.store.oauth2.{ AccessTokenProvider, ClientCredsTokenProvider }

case class a(id: String)
object Test {
  def main(args: Array[String]): Unit = {

    println("######## Test")
    //    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    //    spark.sql("SET hive.mapred.supports.subdirectories=true")
    //    spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
    //    spark.enableLineageTracking()
    //
    //    import spark.implicits._

    val clientId = ""
    val clientKey = ""
    val authTokenEndpoint = ""
    val accountName = "azuredatalakestore.net"
    val provider: AccessTokenProvider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey)
    val client: ADLStoreClient = ADLStoreClient.createClient(accountName, provider)
    if (!client.checkExists("/test_adls_hive_1127/2020/01/10")) {
      println(s"Source Path(ADLS):  is inexistent. Exiting...");
      return ;
    } else {
      println(client.checkExists("/test_adls_hive_1127/2020/01/10"))
    }
    
    println("adl://azuredatalakestore.net/test_adls_hive_1127/2020/01/10".indexOf("/",3))

    //    val df1 = spark.read.option("header", "true").csv("adl://azuredatalakestore.net/raw/au/abs/2016aus-a/*/*/*/*")
    //    val columns1 = df1.schema.map(col => col.name + " String").reduce(_ + "," + _)
    //    val createTableQuery1 = s"create external table raw.2016aus_a($columns1) row format delimited fields terminated by ',' stored as textfile location 'adl://azuredatalakestore.net/raw/au/abs/2016aus-a'"
    //    spark.sql(createTableQuery1)
    //
    //    val df2 = spark.read.option("header", "true").csv("adl://azuredatalakestore.net/raw/au/abs/2016aus-b/*/*/*/*")
    //    val columns2 = df2.schema.map(col => col.name + " String").reduce(_ + "," + _)
    //    val createTableQuery2 = s"create external table raw.2016aus_b($columns2) row format delimited fields terminated by ',' stored as textfile location 'adl://azuredatalakestore.net/raw/au/abs/2016aus-b'"
    //    spark.sql(createTableQuery2)
    //
    //    val df3 = spark.read.option("header", "true").csv("adl://azuredatalakestore.net/raw/au/abs/2016aus-c/*/*/*/*")
    //    val columns3 = df3.schema.map(col => col.name + " String").reduce(_ + "," + _)
    //    val createTableQuery3 = s"create external table raw.2016aus_c($columns3) row format delimited fields terminated by ',' stored as textfile location 'adl://azuredatalakestore.net/raw/au/abs/2016aus-c'"
    //    spark.sql(createTableQuery3)
    //
    //    val df4 = spark.read.option("header", "true").csv("adl://azuredatalakestore.net/raw/au/abs/2016aus-d/*/*/*/*")
    //    val columns4 = df4.schema.map(col => col.name + " String").reduce(_ + "," + _)
    //    val createTableQuery4 = s"create external table raw.2016aus_d($columns4) row format delimited fields terminated by ',' stored as textfile location 'adl://azuredatalakestore.net/raw/au/abs/2016aus-d'"
    //    spark.sql(createTableQuery4)
    //
    //    val df5 = spark.read.option("header", "true").csv("adl://azuredatalakestore.net/raw/au/abs/2016aus-e/*/*/*/*")
    //    val columns5 = df5.schema.map(col => col.name + " String").reduce(_ + "," + _)
    //    val createTableQuery5 = s"create external table raw.2016aus_e($columns5) row format delimited fields terminated by ',' stored as textfile location 'adl://azuredatalakestore.net/raw/au/abs/2016aus-e'"
    //    spark.sql(createTableQuery5)

    //    spark.read.table("profiler.abc").write.insertInto("profiler.abc_target")
  }
}