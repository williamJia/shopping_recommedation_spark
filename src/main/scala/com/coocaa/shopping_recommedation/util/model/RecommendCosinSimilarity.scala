package com.coocaa.shopping_recommedation.util.model

import com.coocaa.shopping_recommedation.util.model.RecommendDatasetStructure.{ItemPref, ItemSimi}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Joe.Kwan on 2020/1/14
 */
object RecommendCosinSimilarity {

  /**
   * 基于物品的协同过滤，余弦相似度计算
   * @param user_item_rating
   * @return
   */
//  def cosinSimilarity(user_item_rating: Dataset[ItemPref]): Dataset[ItemSimi] = {
//
//    import user_item_rating.sparkSession.implicits._
//
//    val userItemidSet = user_item_rating.withColumn("iv", concat_ws(":", $"itemid", $"pref"))
//      .groupBy("userid").agg(collect_set("iv"))
//      .withColumnRenamed("collect_set(iv)", "itemid_set")
//      .select("userid", "itemid_set")
//
//
//    val itemPairsScore = userItemidSet.flatMap { row =>
//      val itemlist = row.getAs[scala.collection.mutable.WrappedArray[String]](1).toArray.sorted
//      val result = new ArrayBuffer[(String, String, Double, Double)]()
//
//      for (i <- 0 to itemlist.length-2){
//        for (j <- i+1 to itemlist.length-1) {
//          result += ((itemlist(i).split(":")(0), itemlist(j).split(":")(0), itemlist(i).split(":")(1).toDouble, itemlist(j).split(":")(1).toDouble))
//        }
//      }
//      result
//    }.withColumnRenamed("_1", "itemidI").withColumnRenamed("_2", "itemidJ")
//      .withColumnRenamed("_3", "scoreI").withColumnRenamed("_4", "scoreJ")
//
//
//    /**
//     *
//     */
//    val itemPairCosineResult = itemPairsScore.withColumn("cnt", lit(1))
//      .groupBy("itemidI", "itemidJ")
//      .agg(sum($"scoreI" * $"scoreJ").as("dot_xy"), sum($"scoreI" * $"scoreI").as("sum_power_x"), sum($"scoreJ"*$"scoreJ").as("sum_power_y"))
//      .withColumn("result", $"dot_xy" / (sqrt($"sum_power_x") * sqrt($"sum_power_y")))
//
//
//    val fullItemPairCosineResult = itemPairCosineResult.select("itemidI", "itemidJ", "result")
//      .union(itemPairCosineResult.select($"itemidJ".as("itemidI"), $"itemidI".as("itemidJ"), $"result".as("result")))
//
//
//    val out = fullItemPairCosineResult.select("itemidI", "itemidJ", "result")
//      .map { row =>
//        val itemidI = row.getAs[String]("itemidI")
//        val itemidJ = row.getAs[String]("itemidJ")
//        val similar = row.getAs[Double]("result")
//        ItemSimi(itemidI, itemidJ, similar)
//      }
//    out
//  }

}
