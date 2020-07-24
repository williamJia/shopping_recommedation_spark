package com.coocaa.shopping_recommedation.util.model

import com.coocaa.shopping_recommedation.util.model.RecommendDatasetStructure.{ItemAssociation, ItemPref}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Joe.Kwan on 2020/2/7
 */

object RecommendAssociationRules extends Serializable {

  /**
   *
   * @param user_item_rating
   * @return
   */
//  def associationRules(user_item_rating: Dataset[ItemPref]): Dataset[ItemAssociation] = {
//
//    import user_item_rating.sparkSession.implicits._
//    val user_itemset_ds = user_item_rating.groupBy("userid").agg(collect_set("itemid")).withColumnRenamed("collect_set(itemid)", "itemid_set")
//
//    // (item: item half matrix)
//    val half_matrix_ds = user_itemset_ds.flatMap{ row =>
//      val itemlist = row.getAs[mutable.WrappedArray[String]](1).toArray.sorted
//      val result = new ArrayBuffer[(String, String, Double)]()
//      for (i <- 0 to itemlist.length -2) {
//        for (j <- i+ 1 to itemlist.length -1) {
//          result += ((itemlist(i), itemlist(j), 1.0))
//        }
//      }
//      result
//    }.withColumnRenamed("_1", "itemidI").withColumnRenamed("_2", "itemidJ").withColumnRenamed("_3", "score")
//
//    // (item: item half matrix frequency)
//    val each_pair_frequency = half_matrix_ds.groupBy("itemidI", "itemidJ").agg(sum("score").as("sumIJ"))
//
//    // (total frequency)
//    val each_item_frequency = user_item_rating.withColumn("score", lit(1)).groupBy("itemid").agg(sum("score").as("score"))
//    val user_all = user_itemset_ds.count
//
//    // support
//    val add_support_ds = each_pair_frequency.select("itemidI", "itemidJ", "sumIJ")
//      .union(each_pair_frequency.select($"itemidJ".as("itemidI"), $"itemidI".as("itemidJ"), $"sumIJ"))
//      .withColumn("support", $"sumIJ" / user_all.toDouble)
//
//    // Confidence
//    val add_confidence_ds = add_support_ds.join(each_item_frequency.withColumnRenamed("itemid", "itemidI").withColumnRenamed("score", "sumI"), "itemidI")
//      .withColumn("confidence", $"sumIJ" / $"sumI")
//
//    // lift
//    val add_lift_ds = add_confidence_ds.join(each_item_frequency.withColumnRenamed("itemid", "itemidJ").withColumnRenamed("score", "sumJ"), "itemidJ")
//      .withColumn("lift", $"confidence" / ($"sumJ"/user_all.toDouble))
//
//    // co_occurrence
//    val add_co_occurrence_ds = add_lift_ds.withColumn("similar", col("sumIJ") / sqrt(col("sumI") * col("sumJ")))
//
//    // output result
//    val output_result = add_co_occurrence_ds.select("itemidI", "itemidJ", "support", "confidence", "lift", "similar").map { row =>
//      val itemidI = row.getString(0)
//      val itemidJ = row.getString(1)
//      val support = row.getDouble(2)
//      val confidence = row.getDouble(3)
//      val lift = row.getDouble(4)
//      val similar = row.getDouble(5)
//      ItemAssociation(itemidI, itemidJ, support, confidence, lift, similar)
//    }
//    output_result
//  }

}
