package com.lb.spark.appstore.daily

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.lb.spark.utils.DateUtils._

/**
  * Created by samsung on 2017/7/11.
  * 数据处理思路:
  * 将所有的数据合并为一张宽表, 数据字段包括 11个维度字段, 17个统计项字段
  * match与impre使用join合并,  action使用broadcast以map数据结构的方式合并
  * 以userID聚合数据, 先统计出单个用户的汇总数据
  * 合并所有的数据
  *
  * impre和action 与macth匹配过程中, 如果没有对应的impre和action相应的数据补0或None
  * 因为使用UserID为key汇总数据,所以存在相应的访问数据对应的独立用户数也存在, 例如: 如果某个用户存在点击数, 则对应的点击独立用户数为1
  */
object AppstoreDailyFor {
  // spark-submit --master yarn-cluster   --num-executors 11   --executor-memory 8G  --executor-cores 4   --driver-memory 1G    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"  --class com.lb.spark.appstore.daily.AppstoreDailyFor  spark-1.0-SNAPSHOT.jar  /   out-spark/304/
  def main(args: Array[String]): Unit = {
    // 绑定入参参数
    val Array(input, output) = args

    val sc = new SparkContext(new SparkConf().setAppName("Appstore Daily FlatMap"))
    val sqlContext = new SQLContext(sc)

    /**
      * 处理下载数据
      * 1. 读取parquet文件
      * 2. 以referLogId和actionType为key,去除重复数据 - 该步骤以去掉
      * 3. 再以referLogId为key, 提取待统计数据
      * 4. groupByKey  聚合数据
      * 5. 将action数据转化成map后, 存入broadcast
      */
    val action_rdd_bc = sc.broadcast(sqlContext.read.parquet("/user/cheil/"+input+"/action/")
      // 先根据referLogId
      .map(line => (line.getAs[String]("referLogId"), line)).groupByKey(20)
      //.map { case (k, v) => (k._1, v.head) }.groupByKey(20)
      .map { case (k, v) =>
        val action_data: Array[Long] = Array[Long](0L,0L,0L,0L,0L)
      // 遍历referLogId下所有的数据
        v.foreach{ line =>
          // 根据actionType去重, 如果存在重复的actionType, 则在数据组中覆盖
           line.getAs[Int]("actionType") match  {
             // 9:点击数[10:点击独立用户数]
             case AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_CLICK =>  action_data(0) = 1L
             // 11:进入主页面数[2:进入主页面独立用户数]
             case AppStoreLogType.APP_STORE_LOG_TYPE_ACTION_TYPE_HOME => action_data(1) = 1L
              //  13:下载数[14:下载独立用户数], 15支付金额(单位:毫)
             case AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_DOWNLOAD => action_data(2) = 1L; action_data(3) = line.getAs[Long]("payPrice")
             //  16:安装完成数[17安装完成独立用户数]
             case AppStoreLogType.APP_LOG_TYPE_ACTION_TYPE_INSTALL => action_data(4) = 1L
             case _ => println(line+"--------------"+"action data error")
          }
      }
      // key: referLogId
      (k, (action_data(0), action_data(1), action_data(2), action_data(3), action_data(4)))
      // 转化为map
    }.collectAsMap())


    /**
      * 处理曝光数据
      * 1. 读取parquet文件
      * 2. 以referLogId为key, 提取待统计数据.
      *    根据业务如果impre与match可以匹配上, 则存在相应的曝光数和曝光独立用户数. 所以为节省计算数据量,impre提取数据为空
      * 3. groupByKey  数据去重
      */
    val impre_rdd = sqlContext.read.parquet("/user/cheil/"+input+"/impre/").map { line =>
      // 7:曝光数，8:曝光独立用户数
      (line.getAs[String]("referLogId"), "")
    }.groupByKey(2000)

    /**
      * 处理匹配数据
      * 1. 读取parquet文件
      * 2. 以appStoreLogId为key, 提取待统计数据(包括维度信息和统计信息)
      * 3. groupByKey  数据去重
      */
    sqlContext.read.parquet("/user/cheil/"+input+"/match/").map { line =>
      // 回填时间段信息, 注意: 此处为隐式转换
      val logTimes: String = line.getAs[Long]("logTimestamp").dateFormat

      //4:匹配数, 5:匹配独立用户数
      val a = if (AppStoreLogType.APP_LOG_TYPE_MATCH_SUCCESS_YES == line.getAs[Int]("isMatched")) 1 else 0

      // 格式(key, (Array[维度信息],(统计信息) ) )
      (line.getAs[String]("appStoreLogId"),
        (Array[String](logTimes.substring(0, 10).trim, line.getAs[Long]("campaignId").toString,
          line.getAs[Long]("strategyId").toString, line.getAs[Long]("creativeId").toString,
          line.getAs[String]("originAdsourceId"), line.getAs[Long]("mediaId").toString,
          line.getAs[Long]("adPosId").toString, line.getAs[String]("zoneCountry"),
          line.getAs[String]("zoneProvince"), line.getAs[String]("zoneCity"), logTimes.substring(11, 13).trim),
          // 1. 请求数 2. 请求独立用户数  3. 匹配数 4. 竞价金额(单位:毫), 5. 成交金额(单位:毫)
          (1, raw"${line.getAs[String]("userId")}", a, line.getAs[Long]("strategyPrice"), line.getAs[Long]("dealPrice"))))
    }.groupByKey(2000)

      /**
        * 关联match, impre和action数据
        *  match数据与impre数采用leftOuterJoin关联
        *  action存储在每隔节点的broadcast中,  使用map的方式通过key 读取相应的数据
        */
      .leftOuterJoin(impre_rdd).map {
      // match和impre数据都存在, 所以将impre的曝光数置为1 . (元组中第六列)
      case (k, (m, Some(i))) => {
        // match数据
        val a = m.head
        //i.size
        if(m.size > 1){println(k+"--------match存在重复数据-------"+m.size)}
        /**
          * 关联action数据, 通过match中的appStoreLogId与action中的referLogId关联, 从map中取出数据.
          * 如果相应的key不存在, 则取默认值
          */
        val c = action_rdd_bc.value.getOrElse(k, (0L, 0L, 0L, 0L, 0L))
        // key: userID
        // value: 维度信息, 请求数, 请求独立用户数,  匹配数, 竞价金额, 成交金额, 曝光数, 点击数  进入主页面数  下载数  支付金额  安装完成数
        (raw"${a._2._2}", (a._1, a._2._1, a._2._3, a._2._4, a._2._5, 1, c._1, c._2, c._3, c._4, c._5))
      }
      // match存在,  impre不存在, 将impre的曝光数置为0
      case (k, (m, None)) => {
        val a = m.head
        val c = action_rdd_bc.value.getOrElse(k, (0L, 0L, 0L, 0L, 0L))
        (raw"${a._2._2}", (a._1, a._2._1, a._2._3, a._2._4, a._2._5, 0, c._1, c._2, c._3, c._4, c._5))
      }
    }

      /**
        * 1. 根据userID汇总数据
        * 2. 计算每个用户的统计报表
        */
      .groupByKey(2000).map{case (k, v) =>
      // 定义中间变量, 缓存数据结果
      val maps : scala.collection.mutable.HashMap[Tuple2[String, String], Array[Long]] = scala.collection.mutable.HashMap()
      v.foreach{line =>
        COUNTER_DIMENSIONS_Map.foreach { case (map_k, map_v) =>
            // key :  报表名称, 统计维度
            val key = Tuple2(map_k,map_v.map(clumon.zip(line._1).toMap.getOrElse(_, "")).mkString(","))
            // value:  请求数, 匹配数, 竞价金额, 成交金额, 曝光数, 点击数  进入主页面数  下载数  支付金额  安装完成数
            val value =  Array[Long](line._2, line._3, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11)
            /**
              * 1. 从map中取出数据, 如果没有对应的key, 返回默认值 (getOrElse)
              * 2. 合并两组数据 (zip),两个数组中对应的数值相加 (map)
              * 3. 将结果跟新回Map  (update)
              */
           maps.update(key , maps.getOrElse(key, Array[Long](0,0,0,0,0,0,0,0,0,0)).zip(value).map(x => x._1 + x._2))
          }
      }
      // 计算相关数据的独立用户数
      maps.map{case (x, a) =>
        (x,  Array(a(0), // 1. 请求数
          1L, // 2. 请求独立用户数
          a(1), // 3. 匹配数
          if (a(1) > 0) 1L else 0, // 4. 匹配独立用户数
          a(2), // 5. 竞价金额(单位:毫)
          a(3), // 6. 成交金额(单位:毫)
          a(4), // 7. 曝光数
          if (a(4) > 0) 1L else 0, // 8. 曝光独立用户数
          a(5), // 9. 点击数
          if (a(5) > 0) 1L else 0, // 10. 点击独立用户数
          a(6), // 11. 进入主页面数
          if (a(6) > 0) 1L else 0, // 12. 进入主页面独立用户数
          a(7), // 13. 下载数
          if (a(7) > 0) 1L else 0, // 14. 下载独立用户数
          a(8), // 15. 支付金额(单位:毫)
          a(9), // 16. 安装完成数,
          if (a(9) > 0) 1L else 0 // 17. 安装完成独立用户数
        ))
      }
    }.flatMap(x=> x)

      /**
        * 根据报表名称和统计项 合并数据, 统计出最终报表
        */
      .reduceByKey((x, y) => (x, y).zipped.map(_ + _), 20).map { case (x, y) => (x._1, raw"${x._2},${y.mkString(",")}") }
      // 根据报表名称多目录存储文件, spark多目录为同一目录下多个以key命名的文件
      .partitionBy(new HashPartitioner(20))
      .saveAsHadoopFile(output, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    // 关闭服务
    sc.stop
  }

  // 所有维度字段, 与数据中前十一个字段对应生成map
  val clumon = Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId", "reportCountry", "reportProvince", "reportCity", "reportSegment")

  // 定义统计维度 Map[表名 -> Array[维度字段]]
  val COUNTER_DIMENSIONS_Map = Map[String, Array[String]](
    "Campaign" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId"),
    "CampaignSegment" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "reportSegment"),
    "CampaignZone" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "reportCountry", "reportProvince", "reportCity"),
    "Media" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId"),
    "MediaSegment" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId", "reportSegment"),
    "MediaZone" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId", "mediaId", "adPosId", "reportCountry", "reportProvince", "reportCity")
    //,
    //"Independent" -> Array("reportDate"),
    //"IndependentSegment" -> Array("reportDate", "reportSegment"),
    //"IndependentZone" -> Array("reportDate", "reportCountry", "reportProvince", "reportCity")
  )
}