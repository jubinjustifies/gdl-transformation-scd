package com.transformation.scd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import za.co.absa.spline.core.SparkLineageInitializer._

object TitanicAgg {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(args(0)).enableHiveSupport().getOrCreate()
    spark.sql("SET hive.mapred.supports.subdirectories=true")
    spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")

    spark.enableLineageTracking()
    
    import spark.implicits._

    spark.table(s"${args(1)}.${args(2)}")
      .filter($"survived" === 1)
      .groupBy("sex")
      .agg(functions.count("sex"))
      .write.mode("overwrite").insertInto("reporting.survival_by_gender")

    spark.table(s"${args(1)}.${args(2)}")
      .filter($"survived" === 1)
      .groupBy("pclass")
      .agg(functions.count("pclass"))
      .write.mode("overwrite").insertInto("reporting.survival_by_pclass")
  }
}