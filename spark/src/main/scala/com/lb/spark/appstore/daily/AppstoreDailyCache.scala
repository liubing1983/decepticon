package com.lb.spark.appstore.daily

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.lb.spark.utils.DateUtils._
import org.apache.spark.storage.StorageLevel

/**
  * Created by samsung on 2017/7/3.
  */
object AppstoreDailyCache {

  // spark-submit --master yarn-cluster   --num-executors 6   --executor-memory 20G  --executor-cores 6   --driver-memory 3G    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"  --class com.lb.spark.appstore.daily.AppstoreDailyCache  spark-1.0-SNAPSHOT.jar  /   out-spark/2/
  def main(args: Array[String]): Unit = {
    // 绑定入参参数
    val Array(input, output) = args

    // 初始化spark上下文
    val sc = new SparkContext(new SparkConf().setAppName("Appstore Daily cache"))
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
      (line.getAs[String]("referLogId"), "")
    }.groupByKey(1200)


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
        (Array[String](logTimes.substring(0, 10).trim,
          line.getAs[Long]("campaignId").toString,
          line.getAs[Long]("strategyId").toString,
          line.getAs[Long]("creativeId").toString,
          line.getAs[String]("originAdsourceId"),
          line.getAs[Long]("mediaId").toString,
          line.getAs[Long]("adPosId").toString,
          line.getAs[String]("zoneCountry"),
          line.getAs[String]("zoneProvince"),
          line.getAs[String]("zoneCity"),
          logTimes.substring(10).trim),
          //  注: 取消批次数统计  1. 请求数 2. 请求独立用户数 3. 请求批次数 4. 匹配数 5. 匹配独立用户数 6. 竞价金额(单位:毫), 7. 成交金额(单位:毫)
          //(1, raw"${line.getAs[String]("userId")}", line.getAs[String]("batchLogId"), a._1, a._2, line.getAs[Long]("strategyPrice"), line.getAs[Long]("dealPrice"))))

          // 1. 请求数 2. 请求独立用户数  3. 匹配数  4. 竞价金额(单位:毫), 5. 成交金额(单位:毫)
          (1, raw"${line.getAs[String]("userId")}", a,  line.getAs[Long]("strategyPrice"), line.getAs[Long]("dealPrice"))))
    }.groupByKey(1200)
      // 关联match和impre数据
      .leftOuterJoin(impre_rdd).map {
      // match和impre数据都存在
      case (k, (m, Some(i))) => {
        val a = m.head
        (k, (a._1, a._2._1, raw"${a._2._2}", a._2._3, a._2._4, a._2._5, 1))
      }
      // match存在,  impre不存在
      case (k, (m, None)) => {
        val a = m.head
        // key: referLogId
        // value: 维度信息, 请求数, 请求独立用户数,  匹配书, 竞价金额, 成交金额, 曝光数, 点击数  进入主页面数  下载数  支付金额  安装完成数
        (k, (a._1, a._2._1, raw"${a._2._2}", a._2._3, a._2._4, a._2._5, 0))
      }
    }.map {
      // 关联action数据
      case (k, a) =>
        //val a = v.head
        // 通过match中的appStoreLogId与action中的referLogId关联, 从map中取出数据
        val c = action_rdd_bc.value.getOrElse(k, (0, 0, 0, 0L, 0))
        // key:  维度信息, 请求独立用户数
        // value: 请求数, 匹配数, 竞价金额, 成交金额, 曝光数, 点击数  进入主页面数  下载数  支付金额  安装完成数
        ((a._1, raw"${a._3}"), Array[Long](a._2, a._4, a._5, a._6, a._7, c._1, c._2, c._3, c._4, c._5))
    }
      // 因内存不足, 将数据缓存在硬盘上
      //.persist(StorageLevel.MEMORY_AND_DISK_2)
      //.persist(StorageLevel.DISK_ONLY_2)
    .cache

    /**
      * 迭代统计维度的, 循环计算各张报表
      */
    COUNTER_DIMENSIONS.foreach { case (map_k, map_v: Array[String]) =>
      match_rdd.map { case (rdd_k, rdd_v) =>
        /**
          * 格式 ((统计维度, userid), 统计数据)
          * 1. 统计维度数据与clumon合并为map, 各报表从map中取出各自需要的统计维度字段
          * 2. userid, 因内存容量限制, 增加suerid减少单次处理的数据
          */
        ((map_v.map(x => clumon.zip(rdd_k._1).toMap.getOrElse(x, "")).mkString(","), rdd_k._2), rdd_v)}
        .aggregateByKey(Array[Long](0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 2000)(seq, comb)
        .map { case (x, y) =>  (x._1, y) }.reduceByKey((x, y) => (x, y).zipped.map(_ + _), 20) .map { case (x, y) => raw"${x},${y.mkString(",")}" } .saveAsTextFile(s"${output}/${map_k}/")
    }
    // 关闭服务
    sc.stop
  }
  def seq(a: Array[Long], b: Array[Long]): Array[Long] = {
    // println(s"""seq: ${a.mkString}  --------   ${b.mkString}""")
    Array(a(0) + b(0), // 1. 请求数
      a(1) + b(1), // 3. 匹配数
      a(2) + b(2), // 5. 竞价金额(单位:毫)
      a(3) + b(3), // 6. 成交金额(单位:毫)
      a(4) + b(4), // 7. 曝光数
      a(5) + b(5), // 9. 点击数
      a(6) + b(6), // 11. 进入主页面数
      a(7) + b(7), // 13. 下载数
      a(8) + b(8), // 15. 支付金额(单位:毫)
      a(9) + b(9) // 16. 安装完成数,
    )
  }

  def comb(a: Array[Long], b: Array[Long]): Array[Long] = {
    Array(a(0) + b(0), // 1. 请求数
      1L,    // 2. 请求独立用户数
      a(1) + b(1), // 3. 匹配数
      if (a(1)  > 0 || b(1) > 0) 1L else 0, // 4. 匹配独立用户数
      a(2) + b(2), // 5. 竞价金额(单位:毫)
      a(3) + b(3), // 6. 成交金额(单位:毫)
      a(4) + b(4), // 7. 曝光数
      if (a(4) > 0 || b(4) > 0) 1L else 0, // 8. 曝光独立用户数
      a(5) + b(5), // 9. 点击数
      if (a(5) > 0 || b(5) > 0) 1L else 0, // 10. 点击独立用户数
      a(6) + b(6), // 11. 进入主页面数
      if (a(6) > 0  || b(6) > 0) 1L else 0, // 12. 进入主页面独立用户数
      a(7) + b(7), // 13. 下载数
      if (a(7) > 0 || b(7) > 0) 1L else 0, // 14. 下载独立用户数
      a(8) + b(8), // 15. 支付金额(单位:毫)
      a(9) + b(9), // 16. 安装完成数
      if (a(9) > 0 || b(9) > 0) 1L else 0 // 17. 安装完成独立用户数
     )
  }


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
    "IndependentZone" -> Array("reportDate", "reportCountry", "reportProvince", "reportCity")
  )
}