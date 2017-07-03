package com.lb.spark.appstore.daily

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.lb.spark.utils.DateUtils._

/**
  * Created by samsung on 2017/7/3.
  */
object AppstoreDailyCache {

  // spark-submit --master yarn-cluster --num-executors 12  --driver-memory 4g --executor-memory 4g  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC"  --class com.lb.spark.appstore.daily.AppstoreDailyCache  spark-1.0-SNAPSHOT.jar  /   out-spark/16/

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

  def main(args: Array[String]): Unit = {
    // 绑定入参参数
    val Array(input, output) = args

    // 初始化spark上下文
    val sc = new SparkContext(new SparkConf().setAppName("AppstoreDaily"))
    val sqlContext = new SQLContext(sc)


    /**
      * 处理下载数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以referLogId和actionType为key,去除重复数据
      * 4. 再以referLogId为key, 提取待统计数据
      * 5. groupByKey  数据去重
      */
    //val action_rdd = sc.textFile("/appstore/flume/action/" + input + "/*/*/*.log").flatMap { line =>
    val action_rdd = sqlContext.read.parquet("/user/cheil/parquet-512/action/")
      .map(line => ((line.getAs[String]("referLogId"), line.getAs[String]("actionType")), line)).groupByKey(1).map { case ((k1, k2), actionLog) =>

      // 10:点击数  11:点击独立用户数
      val a = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_CLICK == actionLog.head.getAs[Int]("actionType")) {
        (1, actionLog.head.getAs[String]("userId"))
      } else {
        (0, "")
      }

      // 12:进入主页面数，13:进入主页面独立用户数
      val b = if (AppStoreLogType.APP_STORE_LOG_TYPE_ACTION_TYPE_HOME == actionLog.head.getAs[Int]("actionType")) {
        (1, actionLog.head.getAs[String]("userId"))
      } else {
        (0, "")
      }

      // 14:下载数,15:下载独立用户数 ,  16支付金额(单位:毫)
      val c = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_DOWNLOAD == actionLog.head.getAs[Int]("actionType")) {
        (1, actionLog.head.getAs[String]("userId"), actionLog.head.getAs[Long]("payPrice"))
      } else {
        (0, "", 0L)
      }

      // 17:安装完成数, 18安装完成独立用户数
      val d = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_INSTALL == actionLog.head.getAs[Int]("actionType")) {
        (1, actionLog.head.getAs[String]("userId"))
      } else {
        (0, "")
      }
      (k1, (a._1, a._2, b._1, b._2, c._1, c._2, c._3, d._1, d._2))
    }

    val action_rdd_bc = sc.broadcast(action_rdd.collectAsMap())


    /**
      * 处理曝光数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以referLogId为key, 提取待统计数据
      * 4. groupByKey  数据去重
      */
    // val impre_rdd = sc.textFile("/appstore/flume/impre/" + input + "/*/*/*.log").flatMap { line =>
    val impre_rdd = sqlContext.read.parquet("/user/cheil/parquet-512/impre/").map { line =>
      // 8:曝光数，9:曝光独立用户数
      (line.getAs[String]("referLogId"), (1, line.getAs[String]("userId")))
    }.groupByKey(600)


    /**
      * 处理匹配数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以appStoreLogId为key, 提取待统计数据(包括维度信息和统计信息)
      * 4. groupByKey  数据去重
      */
    val match_rdd = sqlContext.read.parquet("/user/cheil/parquet-512/match/").map { line =>
      // 回填时间段信息
      val logTimes: String = line.getAs[Long]("logTimestamp").dateFormat

      //4:匹配数, 5:匹配独立用户数
      val a = if (AppStoreLogType.APP_LOG_TYPE_MATCH_SUCCESS_YES == line.getAs[Int]("isMatched")) {
        (1, line.getAs[String]("userId"))
      } else {
        (0, "")
      }

      // 格式(key, (Array[维度信息],(统计信息) ) )
      (line.getAs[String]("appStoreLogId"),
        (Array(logTimes.substring(0, 10).trim,
          line.getAs[Long]("campaignId").toString,
          line.getAs[Long]("strategyId").toString,
          line.getAs[Long]("creativeId").toString,
          line.getAs[String]("originAdsourceId"),
          line.getAs[Long]("mediaId").toString,
          line.getAs[Long]("adPosId").toString,
          line.getAs[String]("zoneCountry"),
          line.getAs[String]("zoneProvince"),
          line.getAs[String]("zoneCity"),
          logTimes.substring(10, 12)),
          // 1. 请求数 2. 请求独立用户数 3. 请求批次数 4. 匹配数 5. 匹配独立用户数 6. 竞价金额(单位:毫), 7. 成交金额(单位:毫)
          (1, line.getAs[String]("userId"), line.getAs[String]("batchLogId"), a._1, a._2, line.getAs[Long]("strategyPrice"), line.getAs[Long]("dealPrice"))))
    }.groupByKey(600).leftOuterJoin(impre_rdd).map {
      case (k, (m, Some(i))) => {
        val a = m.head
        val b = i.head

        (k, (a._1, a._2._1, a._2._2, a._2._3, a._2._4, a._2._5, a._2._6, a._2._7, b._1, b._2))
      }
      case (k, (m, None)) => {
        val a = m.head
        (k, (a._1, a._2._1, a._2._2, a._2._3, a._2._4, a._2._5, a._2._6, a._2._7, 0, ""))
      }
    }.groupByKey(600).map {
      case (k, v) =>
        val a = v.head
        val c = action_rdd_bc.value.getOrElse(k, (0, "", 0, "", 0, "", 0L, 0, ""))
        (a._1, (a._2, a._3, a._4, a._5, a._6, a._7, a._8, a._9, a._10, c._1, c._2, c._3, c._4, c._5, c._6, c._7, c._8, c._9))
    }.cache

    println("数据大小: "+match_rdd.count() +",  样例数据:"+ match_rdd.first()+", key: "+ match_rdd.first()._1.mkString(","))

   // match_rdd.saveAsTextFile(output2)

    /**
      * 计算数据
      */
    COUNTER_DIMENSIONS.foreach { case (map_k, map_v: Array[String]) =>
      match_rdd.map { case (rdd_k: Array[String], rdd_v) =>

        val clumon_table: Map[String, String] = clumon.zip(rdd_k).toMap

        val a = map_v.map(x=> clumon_table.getOrElse(x, "")).mkString(",")
println(a+"============================")
        (a, rdd_v)
      }.groupByKey(600).map {
        case (k, v) =>
          val seq_ = v.toSeq

          if(seq_.size > 1000) println(seq_.size+"----------------------------------------------"+k)

          s"""${k},${seq_.filter(_._1 == 1).size},
             |${seq_.map(_._2).toSet.size},
             |${seq_.map(_._3).toSet.size},
             |${seq_.filter(_._4 == 1).size},
             |${seq_.map(_._5).toSet.size},
             |${seq_.map(_._6).sum},
             |${seq_.map(_._7).sum},
             |${seq_.filter(_._8 == 1).size},
             |${seq_.map(_._9).toSet.size},
             |${seq_.filter(_._10 == 1).size},
             |${seq_.map(_._11).toSet.size},
             |${seq_.filter(_._12 == 1).size},
             |${seq_.map(_._13).toSet.size},
             |${seq_.filter(_._14 == 1).size},
             |${seq_.map(_._15).toSet.size},
             |${seq_.map(_._16).sum},
             |${seq_.filter(_._17 == 1).size},
             |${seq_.map(_._18).toSet.size}""".stripMargin
      }.saveAsTextFile(s"${output}/${map_k}/")
    }
    // 关闭服务
    sc.stop
  }
}
