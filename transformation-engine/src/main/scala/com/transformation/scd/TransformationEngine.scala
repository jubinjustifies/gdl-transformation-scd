package com.transformation.scd

import scala.io.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import za.co.absa.spline.core.SparkLineageInitializer._
import java.io.InputStreamReader
import java.io.File

object TransformationEngine {
  def main(args: Array[String]): Unit = {

    println("######## Engine")
    val mandatoryArgsLength = 2
    if (args.length != mandatoryArgsLength) {
      println("2 arguments are mandatory. <ApplicationName> <ConfigPath>");
      return ;
    }

    val conf: Config = if (args.length == mandatoryArgsLength - 1)
      ConfigFactory.parseReader(new InputStreamReader(getClass.getResourceAsStream("/application.properties")))
    else
      ConfigFactory.parseFile(new File(args(mandatoryArgsLength - 1)))

    val spark = SparkSession.builder().appName(args(0)).enableHiveSupport().getOrCreate()
    spark.sql("SET hive.mapred.supports.subdirectories=true")
    spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")

    if (conf.getBoolean("enable.lineage"))
      spark.enableLineageTracking()

    val censusData = spark.read.table(conf.getString("tableName"))
    val res = censusData.groupBy(conf.getString("groupByColumn")).agg(functions.count(functions.col(conf.getString("aggregateColumn"))).alias("count_" + conf.getString("aggregateColumn"))).select(conf.getString("groupByColumn"), "count_" + conf.getString("aggregateColumn"))
    val resCols = res.schema.map(col => col.name + " String").reduce(_ + "," + _)
    val createAggTableQuery = s"create external table if not exists ${conf.getString("tableName")}_agg($resCols) row format delimited fields terminated by ',' stored as textfile location 'adl://pepperadlsuat.azuredatalakestore.net/raw/au/test/${if (conf.getString("tableName").split("\\.").length == 1) conf.getString("tableName") else conf.getString("tableName").split("\\.")(1)}_agg'"
    spark.sql(createAggTableQuery)
    res.write.insertInto(s"${conf.getString("tableName")}_agg")

  }
}