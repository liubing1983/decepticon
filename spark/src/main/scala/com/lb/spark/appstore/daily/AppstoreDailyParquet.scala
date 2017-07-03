package com.lb.spark.appstore.daily

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.lb.spark.utils.DateUtils._
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}



/**
  * Created by samsung on 2017/6/20.
  */
object AppstoreDailyParquet {
  // spark-submit --master yarn-cluster  --num-executors 20  --driver-memory 6g --executor-memory 3g   --class com.lb.spark.appstore.daily.AppstoreDaily  spark-1.0-SNAPSHOT.jar  2017008   out-spark/1/
  // spark-submit --master yarn-cluster --num-executors 12  --driver-memory 4g --executor-memory 4g  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC"  --class com.lb.spark.appstore.daily.AppstoreDailyParquet  spark-1.0-SNAPSHOT.jar  /   out-spark/16/

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
      case (k, (ma, Some(i))) => (ma._1, (ma._2, ma._3, i.head._1, i.head._2))
      // 曝光日志不存在
      case (k, (ma, None)) => (ma._1, (ma._2, ma._3, 0, ""))
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
    }.flatMap(line => line).groupByKey(10000).map {
      case (k, v) =>
        // 将合并的数据转换为seq
        val seq_ = v.toSeq
        //println(k._1+"-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
        // 计算各个指标
        (k._1, s"""${k._2},${seq_.filter(_._1._1 == 1).size},
           |${seq_.map(_._1._2).toSet.size},
           |${seq_.map(_._1._3).toSet.size},
           |${seq_.filter(_._1._4 == 1).size},
           |${seq_.map(_._1._5).toSet.size},
           |${seq_.map(_._1._6).sum},
           |${seq_.map(_._1._7).sum},
           |${seq_.filter(_._3 == 1).size},
           |${seq_.map(_._4).toSet.size},
           |${seq_.filter(_._2._1 == 1).size},
           |
           |${seq_.map(_._2._2).toSet.size},
           |${seq_.filter(_._2._3 == 1).size},
           |
           |${seq_.map(_._2._4).toSet.size},
           |${seq_.filter(_._2._5 == 1).size},
           |${seq_.map(_._2._6).toSet.size},
           |${seq_.map(_._2._7).sum},
           |${seq_.filter(_._2._8 == 1).size},
           |${seq_.map(_._2._9).toSet.size}""".stripMargin)
      //}.saveAsTextFile(s"${output}/${k._}/")
      // 根据key 多目录存储到hdfs(注意: spark多目录是在同一目录生成多个文件)
    }.saveAsHadoopFile(output, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    // 关闭服务
    sc.stop
  }
}