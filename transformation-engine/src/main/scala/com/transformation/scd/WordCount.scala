package com.transformation.scd

import org.apache.spark.sql.SparkSession
import za.co.absa.spline.core.SparkLineageInitializer._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import za.co.absa.spline.core.conf.SplineConfigurer

object WordCount {
  def main(args: Array[String]): Unit = {

    println("## WordCount")
    val spark = SparkSession.builder().appName(args(0)).enableHiveSupport().getOrCreate()
    spark.sql("SET hive.mapred.supports.subdirectories=true")
    spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")

    import spark.implicits._
//    spark.enableLineageTracking()

    val df = spark.read.option("header", "true").csv("/tmp/words.csv")

    val hdfs = FileSystem.get(new URI(""), new Configuration)
    if (hdfs.exists(new Path("/tmp/sample")))
      hdfs.delete(new Path("/tmp/sample"), true)

    df.distinct.sort("id").write.csv(args(1))

  }
}