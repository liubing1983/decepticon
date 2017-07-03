package com.lb.spark.appstore.daily

import java.text.SimpleDateFormat

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import com.cheil.pengtai.appstore.pojo.log.AppStoreLog
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jackson.map.ObjectMapper


/**
  * Created by samsung on 2017/6/5.
  * 数据处理思路:
  * 将所有的数据合并为一张宽表, 数据字段包括 11个维度字段, 18个统计项字段
  * impre和action 与macth匹配过程中, 如果没有对应的impre和action相应的数据补0或null
  */
object AppstoreDaily {

  // spark-submit --master yarn-cluster   --class com.lb.spark.appstore.daily.AppstoreDaily  spark-1.0-SNAPSHOT.jar  2017008   out-spark/1/

  // 时间格式化样式
  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // 所有维度字段, 与数据中前十一个字段对应生成map
  val clumon = Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId", "reportCountry", "reportProvince", "reportCity", "reportSegment")

  // 定义统计维度
  val COUNTER_DIMENSIONS = Map[String, Array[String]](
    "Campaign" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId"),
    "CampaignSegment" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "reportSegment"),
    "CampaignZone" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "reportCountry", "reportProvince", "reportCity"),
    "Media" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId"),
    "MediaSegment" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId", "reportSegment"),
    "MediaZone" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId", "reportCountry", "reportProvince", "reportCity"),
    "Independent" -> Array("reportDate"),
    "IndependentSegment" -> Array("reportDate", "reportSegment"),
    "IndependentZone" -> Array("reportDate", "reportCountry", "reportProvince", "reportCity"))

  val mapper: ObjectMapper = new ObjectMapper

  def main(args: Array[String]): Unit = {

    // 绑定入参参数
    val Array(input, output) = args

    // 初始化spark上下文
    val sc = new SparkContext(new SparkConf().setAppName("AppstoreDaily"))

    /**
      * 处理曝光数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以referLogId为key, 提取待统计数据
      * 4. groupByKey  数据去重
      */
   // val impre_rdd = sc.textFile("/appstore/flume/impre/" + input + "/*/*/*.log").flatMap { line =>
      val impre_rdd = sc.textFile("/user/cheil/out/impre/").flatMap { line =>
      //映射
      try {
        Some(mapper.readValue(line, classOf[AppStoreLog]))
      } catch {
        case e: Exception => None
      }
    }.filter(isValid(_, "2")).map { impreLog =>
                              // 8:曝光数，9:曝光独立用户数
      (impreLog.getReferLogId, (1, impreLog.getUserId))
    }.groupByKey


    /**
      * 处理匹配数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以appStoreLogId为key, 提取待统计数据(包括维度信息和统计信息)
      * 4. groupByKey  数据去重
      */
    val match_rdd = sc.textFile("/user/cheil/out/match/").flatMap { line =>
    // val match_rdd = sc.textFile("/appstore/flume/match/" + input + "/*/*/*.log").flatMap { line =>
      //映射
      try {
        Some(mapper.readValue(line, classOf[AppStoreLog]))
      } catch {
        case e: Exception => None
      }
    }.filter(isValid(_)).map { matchedLog =>
      // 回填时间段信息
      val logTimes: String = sdf.format(matchedLog.logTimestamp)

      //4:匹配数, 5:匹配独立用户数
      val a = if (AppStoreLogType.APP_LOG_TYPE_MATCH_SUCCESS_YES == matchedLog.isMatched) {
        (1, matchedLog.userId)
      } else {
        (0, null)
      }

      // 格式(key, (Array[维度信息],(统计信息) ) )
      (matchedLog.appStoreLogId,
        (Array(logTimes.substring(0, 10).trim, matchedLog.campaignId.toString, matchedLog.strategyId.toString, matchedLog.creativeId.toString,
          matchedLog.originAdsourceId, matchedLog.mediaId, matchedLog.adPosId, matchedLog.zoneCountry,
          matchedLog.zoneProvince, matchedLog.zoneCity, logTimes.substring(10,12)),
          // 1. 请求数 2. 请求独立用户数 3. 请求批次数 4. 匹配数 5. 匹配独立用户数 6. 竞价金额(单位:毫), 7. 成交金额(单位:毫)
          (1, matchedLog.userId, matchedLog.batchLogId, a._1, a._2, matchedLog.strategyPrice, matchedLog.dealPrice)))
    }.groupByKey

    /**
      * 处理下载数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以referLogId和actionType为key,去除重复数据
      * 4. 再以referLogId为key, 提取待统计数据
      * 5. groupByKey  数据去重
      */
    val action_rdd = sc.textFile("/user/cheil/out/action/").flatMap { line =>
    //val action_rdd = sc.textFile("/appstore/flume/action/" + input + "/*/*/*.log").flatMap { line =>
      //映射
      try {
        Some(mapper.readValue(line, classOf[AppStoreLog]))
      } catch {
        case e: Exception => None
      }
    }.filter(isValid(_, "3")).map(line => ((line.referLogId, line.actionType), line)).groupByKey.map { case ((k1, k2), actionLog) =>

      // 10:点击数  11:点击独立用户数
      val a = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_CLICK == actionLog.head.actionType) {
        (1, actionLog.head.userId)
      } else {
        (0, null)
      }

      // 12:进入主页面数，13:进入主页面独立用户数
      val b = if (AppStoreLogType.APP_STORE_LOG_TYPE_ACTION_TYPE_HOME == actionLog.head.actionType) {
        (1, actionLog.head.userId)
      } else {
        (0, null)
      }

      // 14:下载数,15:下载独立用户数 ,  16支付金额(单位:毫)
      val c = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_DOWNLOAD == actionLog.head.actionType) {
        (1, actionLog.head.userId, actionLog.head.payPrice)
      } else {
        (0, null, 0L)
      }

      // 17:安装完成数, 18安装完成独立用户数
      val d = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_INSTALL == actionLog.head.actionType) {
        (1, actionLog.head.userId)
      } else {
        (0, null)
      }

      (k1, (a._1, a._2, b._1, b._2, c._1, c._2, c._3, d._1, d._2))
    }.groupByKey

    /**
      * 将三份数据合并成宽表
      */
    // 合并请求和点击数据
    val all_data = match_rdd.join(action_rdd).map {
      // 匹配日志和点击日志都存在
      case (k, (m, a)) => (k, ((m.head), (a.head)))
      // 匹配日志存在, 点击日志不存在
      case (k, (m, null)) => (k, ((m.head), ((0, null, 0, null, 0, null, 0L, 0, null))))
    }.join(impre_rdd).map { // 合并曝光数据
      // 存在曝光日志
      case (k, (ma, i)) => ((ma._1._1), (ma._1._2, ma._2, i.head._1, i.head._2))
        // 曝光日志不存在
      case (k, (ma, null)) => ((ma._1._1), (ma._1._2, ma._2, 0, null))
    }.map{ line =>
      // 将统计维度的key和值合并为一个map, 供后面的查询使用
      (clumon.zip(line._1).toMap, (line._2))
    }

    /**
      * 循环遍历统计表
      */
    for ((k, v) <- COUNTER_DIMENSIONS) {
      all_data.map { case (maps, line) =>
        // 从map中获取每隔表的地段对应的数据
        ((v.map { vv => maps.getOrElse(vv, "") }).mkString(","), line)
      }.groupByKey(20).map { case (k1, v1) =>
        val seq_ = v1.toSeq
        s"""${k1},
        // match 数据 共7个统计项
        ${seq_.filter(_._1._1 == 1).size},  // 1. 请求数
        ${seq_.map(_._1._2).toSet.size},    // 2. 请求独立用户数
        ${seq_.map(_._1._3).toSet.size},   // 3. 请求批次数
        ${seq_.filter(_._1._4 == 1).size},  // 4. 匹配数
        ${seq_.map(_._1._5).toSet.size},    // 5. 匹配独立用户数
        ${seq_.map(_._1._6).sum},           // 6. 竞价金额(单位:毫),
        ${seq_.map(_._1._7).sum},           // 7. 成交金额(单位:毫)

        // impre 数据 共2个统计项
        ${seq_.filter(_._3 == 1).size},     // 8. 曝光数
        ${seq_.map(_._4).toSet.size},       // 9. 曝光独立用户数

        // action 数据  共9个统计项
        ${seq_.filter(_._2._1 == 1).size},  // 10. 点击数
        ${seq_.   map(_._2._2).toSet.size},    // 11. 点击独立用户数
        ${seq_.filter(_._2._3 == 1).size},  // 12. 进入主页面数
        ${seq_.   map(_._2._4).toSet.size},    // 13. 进入主页面独立用户数
        ${seq_.filter(_._2._5 == 1).size},  // 14. 下载数
        ${seq_.   map(_._2._6).toSet.size},   // 15. 下载独立用户数
        ${seq_.   map(_._2._7).sum},          // 16. 支付金额
        ${seq_.filter(_._2._8 == 1).size},  // 17. 安装完成数
        ${seq_.   map(_._2._9).toSet.size}""" // 18. 安装完成独立用户数
      }.saveAsTextFile(s"${output}/${k}/")
    }
  }



  // 效验数据
  def isValid(appStoreLog: AppStoreLog): Boolean = {
    // 日志类型
    val logType = appStoreLog.logType
    if (logType == null || logType < 0) return false
    if ((logType != AppStoreLogType.APP_LOG_TYPE_MATCH) && (logType != AppStoreLogType.APP_LOG_TYPE_IMPRESSION) && (logType != AppStoreLogType.APP_LOG_TYPE_ACTION)) return false
    // 日志ID
    if (appStoreLog.appStoreLogId == null || appStoreLog.appStoreLogId.toString.isEmpty) return false
    // 用户ID
    //if (appStoreLog.getUserId() == null || appStoreLog.getUserId().toString().isEmpty())   return false
    // 版位ID
    if (appStoreLog.adPosId == null || appStoreLog.adPosId < 0) return false
    //app id
    if (appStoreLog.appId == null || appStoreLog.appId < 0) return false

    return true
  }

  // 效验数据
  def isValid(appStoreLog: AppStoreLog, logType123: String): Boolean = {
    // 必须有来源
    if (appStoreLog.referLogId == null || appStoreLog.referLogId.toString.isEmpty) return false
      isValid(appStoreLog)
    return true
  }
}