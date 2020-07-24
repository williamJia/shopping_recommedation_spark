package com.coocaa.shopping_recommedation.data

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit}

object userItemScore {

  def get_day(n:Int): String ={
    val ymdFormat=new SimpleDateFormat("yyyy-MM-dd")
    val day:Calendar = Calendar.getInstance()
    day.add(Calendar.DATE,-n)
    val logDate = ymdFormat.format(day.getTime)
    logDate
  }

  def main(args: Array[String]): Unit = {

    val day = get_day(30) // 获取一个月前的日期
    val yesterday = get_day(1) // 获取昨天的日期

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
    // 获取一个月内的评分矩阵数据
    val df = spark.sql(
      s"""
         |select a.mac as mac,a.goodid as goodid,
         |  CASE
         |    when a.action=='product_exposure' then '0' -- 曝光商品
         |    when a.action=='product_info' then '1.7' -- 详情页曝光商品
         |    when a.action=='product_cart' then '3.4' -- 加入购物车商品
         |    when a.action=='product_buy' then '5' -- 下单购买的商品
         |  End as actiond
         | FROM (
         |  select mac as mac,goodid as goodid,action as action
         |    from coocaa_adl.adl_ccmall_user_active_commodity
         |    lateral view explode(split(commodity_set, ',')) tmpTable as goodid
         |    where dt>'$day'
         |) as a GROUP BY mac,goodid,action
      """.stripMargin).withColumn("dt", lit(yesterday))

    df.show()

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    df.write.mode(SaveMode.Append).saveAsTable("test.shopping_ui_score")

    spark.stop()
    println("write over")
  }
}
