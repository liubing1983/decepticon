package com.lb.spark.appstore.daily

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import com.lb.spark.utils.DateUtils._

/**
  * Created by samsung on 2017/7/10.
  */
object AppstoreDailyGroupBy {
  // spark-submit --master yarn-cluster   --num-executors 6   --executor-memory 20G  --executor-cores 6   --driver-memory 3G    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"  --class com.lb.spark.appstore.daily.AppstoreDailyCache  spark-1.0-SNAPSHOT.jar  /   out-spark/2/
  def main(args: Array[String]): Unit = {
    // 绑定入参参数
    val Array(input, output) = args

    // 初始化spark上下文
    val sc = new SparkContext(new SparkConf().setAppName("Appstore Daily GrouBY"))
    val sqlContext = new SQLContext(sc)



    /**
      * 处理下载数据
      * 1. 读取parquet文件
      * 2. 以referLogId和actionType为key,去除重复数据
      * 3. 再以referLogId为key, 提取待统计数据
      * 4. groupByKey  数据去重
      */
    val action_rdd = sqlContext.read.parquet("/user/cheil/parquet-512/action/")
      // 先根据referLogId和actionType 去重
      .map(line => ((line.getAs[String]("referLogId"), line.getAs[String]("actionType")), line)).groupByKey(30)
      // 过滤后的数据再以referLogId去重
      .map { case ((k1, k2), actionLog) =>

      // 9:点击数  10:点击独立用户数
      val a = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_CLICK == actionLog.head.getAs[Int]("actionType")) 1 else 0

      // 11:进入主页面数，12:进入主页面独立用户数
      val b = if (AppStoreLogType.APP_STORE_LOG_TYPE_ACTION_TYPE_HOME == actionLog.head.getAs[Int]("actionType")) 1 else 0

      // 13:下载数,14:下载独立用户数 ,  15支付金额(单位:毫)
      val c = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_DOWNLOAD == actionLog.head.getAs[Int]("actionType")) {
        (1, actionLog.head.getAs[Long]("payPrice"))
      } else {
        (0, 0L)
      }

      // 16:安装完成数, 17安装完成独立用户数
      val d = if (AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_INSTALL == actionLog.head.getAs[Int]("actionType")) 1 else 0

      // key: referLogId
      // value: 点击数  进入主页面数  下载数  支付金额  安装完成数
      (k1, (a, b, c._1, c._2, d))
    }

    // 将action数据转化成map后, 存入broadcast
    val action_rdd_bc = sc.broadcast(action_rdd.collectAsMap())

    /**
      * 处理曝光数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以referLogId为key, 提取待统计数据
      * 4. groupByKey  数据去重
      */
    val impre_rdd = sqlContext.read.parquet("/user/cheil/parquet-512/impre/").map { line =>
      // 7:曝光数，8:曝光独立用户数
      (line.getAs[String]("referLogId"))
    }.distinct(1200)


    /**
      * 处理匹配数据
      * 1. 使用Some效验数据是否符合json格式
      * 2. 使用isValid过滤错误数据
      * 3. 以appStoreLogId为key, 提取待统计数据(包括维度信息和统计信息)
      * 4. groupByKey  数据去重
      */
    val match_rdd = sqlContext.read.parquet("/user/cheil/parquet-512/match/").map { line =>
      // 回填时间段信息, 注意: 此处为隐式转换
      val logTimes: String = line.getAs[Long]("logTimestamp").dateFormat

      //4:匹配数, 5:匹配独立用户数
      val a = if (AppStoreLogType.APP_LOG_TYPE_MATCH_SUCCESS_YES == line.getAs[Int]("isMatched")) 1  else 0

      // 格式(key, (Array[维度信息],(统计信息) ) )
      (line.getAs[String]("appStoreLogId"),
        Array[String](logTimes.substring(0, 10).trim,
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

          // 1. 请求数 2. 请求独立用户数  3. 匹配数  4. 竞价金额(单位:毫), 5. 成交金额(单位:毫)
          (1, raw"${line.getAs[String]("userId")}", a,  line.getAs[Long]("strategyPrice"), line.getAs[Long]("dealPrice")))
    }.distinct(1200)

  }

}