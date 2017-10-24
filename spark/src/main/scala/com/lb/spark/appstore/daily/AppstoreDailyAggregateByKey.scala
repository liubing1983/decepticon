package com.lb.spark.appstore.daily

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.lb.spark.utils.DateUtils._
import org.apache.commons.lang.StringUtils

/**
  * Created by samsung on 2017/7/2.
  */
object AppstoreDailyAggregateByKey {

  // spark-submit  --master yarn-cluster   --num-executors 20   --executor-memory 8G  --executor-cores 3   --driver-memory 3G      --conf spark.storage.memoryFraction=0.5  --conf spark.shuffle.memoryFraction=0.3   --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"  --class com.lb.spark.appstore.daily.AppstoreDailyAggregateByKey  spark-1.0-SNAPSHOT.jar  /   out-spark/12/

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
          line.getAs[Long]("campaignId"),
          line.getAs[Long]("strategyId"),
          line.getAs[Long]("creativeId"),
          line.getAs[String]("originAdsourceId"),
          line.getAs[Long]("mediaId"),
          line.getAs[Long]("adPosId"),
          line.getAs[String]("zoneCountry"),
          line.getAs[String]("zoneProvince"),
          line.getAs[String]("zoneCity"),
          logTimes.substring(10, 12)),
          // 1. 请求数 2. 请求独立用户数 3. 请求批次数 4. 匹配数 5. 匹配独立用户数 6. 竞价金额(单位:毫), 7. 成交金额(单位:毫)
          (1, line.getAs[String]("userId"), line.getAs[String]("batchLogId"), a._1, a._2, line.getAs[Long]("strategyPrice"), line.getAs[Long]("dealPrice"))))
    }.groupByKey(600)

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
      .map(line => ((line.getAs[String]("referLogId"), line.getAs[Int]("actionType")), line)).groupByKey(1).map { case ((k1, k2), actionLog) =>

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

    /**
      * 将三份数据合并成宽表
      */
    // 合并请求和点击数据
    match_rdd.leftOuterJoin(action_rdd).map {
      // 匹配日志和点击日志都存在
      case (k, (m, Some(a))) => (k, (m.head._1, m.head._2, a))
      // 匹配日志存在, 点击日志不存在
      case (k, (m, None)) => (k, (m.head._1, m.head._2, (0, "", 0, "", 0, "", 0L, 0, "")))
      //case (k, (None, Some(a))) => (k, (Array(None, 0L, 0L, 0L, None, 0L, 0L, None, None, None, None), (0, None, None, 0, None, 0L, 0L), a))
    }.leftOuterJoin(impre_rdd).map { // 合并曝光数据
      // 存在曝光日志
      case (k, (ma, Some(i))) => (ma._1, Array(ma._2._1.toString,ma._2._2.toString,ma._2._3.toString,ma._2._4.toString,ma._2._5.toString,ma._2._6.toString,ma._2._7.toString,i.head._1.toString,i.head._2.toString,ma._3._1.toString,ma._3._2.toString,ma._3._3.toString,ma._3._4.toString,ma._3._5.toString,ma._3._6.toString,ma._3._7.toString,ma._3._8.toString,ma._3._9.toString))
      // 曝光日志不存在
      case (k, (ma, None)) => (ma._1, Array(ma._2._1.toString,ma._2._2.toString,ma._2._3.toString,ma._2._4.toString,ma._2._5.toString,ma._2._6.toString,ma._2._7.toString, "0", "",ma._3._1.toString,ma._3._2.toString,ma._3._3.toString,ma._3._4.toString,ma._3._5.toString,ma._3._6.toString,ma._3._7.toString,ma._3._8.toString,ma._3._9.toString))
      // case (k, (None, Some(i))) => (Array(None, 0L, 0L, 0L, None, 0L, 0L, None, None, None, None), ((0, None, None, 0, None, 0L, 0L), (0, None, 0, None, 0, None, 0L, 0, None), i._1, i._2))
    }
      // 目前不会出现需要过滤的数据, 暂时注释
      //.filter(_._1(0) != None)
      .map { case (k, rdd_v) =>
      // 将统计维度的key和值合并为一个map, 供后面的查询使用
      val rdd_k: Map[String, Any] = clumon zip k toMap

      // 循环报表, 此处将生成9条数据
      COUNTER_DIMENSIONS.map { case (map_k, map_v) =>
        ((map_k, map_v.map(rdd_k.getOrElse(_, "")).mkString(",")), rdd_v)
      }
      // 将9条数据展开, 根据各自的key  合并数据
    }.flatMap(line => line).aggregateByKey(Array[String]("0","","","0","","0","0","0","","0","","0","","0","","0","0",""), 10000)(seq, comb)
      .map{ case (x , y) =>
        (x._1, s"""${x._2},
            |${y(0)},
            |${y(1).split(",").toSet.size},
            |${y(2).split(",").toSet.size},
            |${y(3)},
            |${y(4).split(",").toSet.size},
            |${y(5)},
            |${y(6)},
            |${y(7)},
            |${y(8).split(",").toSet.size},
            |${y(9)},
            |${y(10).split(",").toSet.size},
            |${y(11)},
            |${y(12).split(",").toSet.size},
            |${y(13)},
            |${y(14).split(",").toSet.size},
            |${y(15)},
            |${y(16)},
            |${y(17).split(",").toSet.size}""".stripMargin)
      }.saveAsHadoopFile(output, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    // 关闭服务
    sc.stop
  }

  def seq(a:Array[String], b:Array[String]) : Array[String] ={
    println(s"""seq: ${a.mkString}  --------   ${b.mkString}""")
    Array(jia(a(0), b(0)), merage(a(1), b(1)), merage(a(2), b(2)), jia(a(3), b(3)), merage(a(4), b(4)),
      jia(a(5), b(5)), jia(a(6), b(6)), jia(a(7), b(7)),
      merage(a(8), b(8)), jia(a(9), b(9)), merage(a(10), b(10)), jia(a(11), b(11)), merage(a(12), b(12)),
      jia(a(13), b(13)), merage(a(14), b(14)), jia(a(15), b(15)), jia(a(16), b(16)), merage(a(17), b(17)))
  }


  def comb(a:Array[String], b:Array[String]) : Array[String] ={
    println(s"""comb: ${a.mkString}  --------   ${b.mkString}""")
    Array(jia(a(0), b(0)), merage(a(1), b(1)), merage(a(2), b(2)), jia(a(3), b(3)), merage(a(4), b(4)),
      jia(a(5), b(5)), jia(a(6), b(6)), jia(a(7), b(7)),
      merage(a(8), b(8)), jia(a(9), b(9)), merage(a(10), b(10)), jia(a(11), b(11)), merage(a(12), b(12)),
      jia(a(13), b(13)), merage(a(14), b(14)), jia(a(15), b(15)), jia(a(16), b(16)), merage(a(17), b(17)))
  }

  /**
    * 加法
    * @param x
    * @param y
    * @return
    */
  def jia(x: String = "0", y: String = "0") : String ={
    (x.toLong + y.toLong).toString
  }

  /**
    * 合并
    * @param x
    * @param y
    * @return
    */
  def merage(x: String, y: String) : String={
    val a = s"${x},${y}".split(",").toSet.filter(x => StringUtils.isNotEmpty(x))
    a.mkString(",")
  }

}
