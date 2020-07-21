package com.coocaa.shopping_recommedation.util.model

/**
 * Created by Joe.Kwan on 2020/1/14
 */
object RecommendDatasetStructure {

  /**
   *
   * @param userid
   * @param itemid
   * @param pref
   */
  case class ItemPref(userid: String, itemid: String, pref: Double)

  /**
   *
   * @param itemidI
   * @param itemidJ
   * @param support
   * @param confidence
   * @param lift
   * @param similar
   */
  case class ItemAssociation(itemidI: String, itemidJ: String, support: Double, confidence: Double, lift: Double, similar: Double)

  /**
   *
   * @param itemidI
   * @param itemidJ
   * @param similar
   */
  case class ItemSimi(itemidI: String, itemidJ: String, similar: Double)

  /**
   *
   * @param userid
   * @param itemid
   * @param pref
   */
  case class UserRecomm(userid: String, itemid: String, pref: Double)

  /**
   *
   * @param did
   * @param video_id
   * @param source
   * @param rating
   * @param rank
   */
  case class DidVidRatingRank(did: String, video_id: String, source: String, rating: Double, rank: Long)

  /**
   *
   * @param methods
   * @param metrics
   */
  case class PredictMetrics(methods: String, metrics: Double)

  /**
   *
   * @param did
   * @param set_test_size
   * @param set_predict_size
   * @param video_intersect_size
   */
  case class DidRecallSize(did: String, set_test_size: Int, set_predict_size: Int, video_intersect_size: Int)


  /**
   *
   * @param iid
   * @param source
   * @param iid_recommend
   * @param similarity
   * @param is_vip
   * @param rank
   */
  case class TeleplayValidInfoForRecommend(iid: String, source: String, iid_recommend: String, similarity: Double, is_vip: Long, rank: Long)


  /**
   *
   * @param vid
   * @param source
   * @param vid_ori
   */
  case class TeleplayTitleCompare(vid: String, source: String, vid_ori: String)


  /**
   *
   * @param vid
   * @param source
   */
  case class TeleplayVidSourceInfo(vid: String, source: String)


  /**
   *
   * @param vid
   * @param title
   * @param source
   * @param vip
   * @param area
   * @param play_count
   * @param score
   * @param hot
   * @param min_episode
   * @param nums_episode
   * @param min_dur
   * @param max_dur
   * @param avg_dur
   * @param sum_dur
   */
  case class AnimationVidInfoForRecommend(vid: String,title: String,source: String,vip: Int,area: String,play_count: Long,score: Double,
                                          hot: Double, min_episode: Long,max_episode: Long,nums_episode: Long,min_dur:Long,max_dur:Long,
                                          avg_dur:Double,sum_dur:Long)


  /**
   *
   * @param vidI
   * @param vidJ
   */
  case class AnimationVideoReplaced(vidI: String, vidJ: String)


  /**
   *
   * @param vid
   * @param source
   * @param vip
   */
  case class AnimationVideoSourceVipInfo(vid: String, source: String, vip: Int)


  /**
   *
   * @param vid
   */
  case class SingleVid(vid: String)

}
