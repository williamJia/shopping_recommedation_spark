package com.coocaa.shopping_recommedation.rec

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object hot {

  def get_yesterday(): String ={
    val ymdFormat=new SimpleDateFormat("yyyy-MM-dd")
    val day:Calendar = Calendar.getInstance()
    day.add(Calendar.DATE,-1)
    val logDate = ymdFormat.format(day.getTime)
    logDate
  }

  def main(args: Array[String]): Unit = {

    val yesterday = get_yesterday()

    val spark: SparkSession = SparkSession.builder()
      .config("spark.default.parallelism", "600")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.speculation.interval", "10000ms")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.shuffle.partitions", "600")
      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("set spark.sql.adaptive.enabled=true")
    val df = spark.sql(
      s"""
         |SELECT a.goodid as goodid,COUNT(a.goodid) as num FROM (
         |  select mac,goodid
         |    from coocaa_adl.adl_ccmall_user_active_commodity
         |    lateral view explode(split(commodity_set, ',')) tmpTable as goodid
         |    where dt='$yesterday'
         |) as a GROUP BY a.goodid ORDER BY num DESC limit 500
      """.stripMargin)
      .select("goodid")
      .withColumn("dt",lit(yesterday))
      .groupBy(col("dt"))
      .agg(collect_list(col("goodid")).as("goodid_arr"))
      .withColumn("goodid",concat_ws(",",col("goodid_arr")))
      .select(col("dt"),col("goodid"))

    df.show()

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    df.write.mode(SaveMode.Append).saveAsTable("recommendation.ccmall_product_hot")

    spark.stop()
    println("write over")

  }
}
