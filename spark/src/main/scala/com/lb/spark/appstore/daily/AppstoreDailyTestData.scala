package com.lb.spark.appstore.daily

import com.cheil.pengtai.appstore.console.dp.analysis.day.common.AppStoreLogType
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by samsung on 2017/7/14.
  */
object AppstoreDailyTestData {
  // spark-submit --master yarn-cluster   --num-executors 11   --executor-memory 8G  --executor-cores 4   --driver-memory 1G    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"  --class com.lb.spark.appstore.daily.AppstoreDailyTestData  spark-1.0-SNAPSHOT.jar  parquet-256   out-spark/304/
  def main(args: Array[String]): Unit = {
    // 绑定入参参数
    val Array(input, output) = args

    val sc = new SparkContext(new SparkConf().setAppName("Appstore Daily FlatMap"))
    val sqlContext = new SQLContext(sc)


    // 6097,19739,14384

    //"Campaign" -> Array("reportDate", "campaignId", "strategyId", "creativeId", "orignaAdsourceId"),

//    val impre_rdd = sqlContext.read.parquet("/user/cheil/" + input + "/impre/")
//
//    println(impre_rdd.count() + "  -- impre 数据量")
//
//    val impre_rdd_group = impre_rdd.map { line =>
//      (line.getAs[String]("referLogId"), (line.getAs[String]("userId"), line.getAs[String]("userIp")))
//    }.filter(_._2._1 != "").map {
//      case (x, y) => (x, s"${y._1}--${y._2}")
//    }.groupByKey(2000)
//
//    println(impre_rdd_group.count() + "  -- impre 根据 referLogId 去重后数据量")
//
//    val impre = impre_rdd_group.map { case (k, line) =>
//      // 7:曝光数，8:曝光独立用户数
//      (line.head, "")
//    }.groupByKey.filter(_._2.size >= 5000).map(x => (x._1, x._2.size))
//
//    impre.saveAsTextFile(output + "/impre/")

    // ================================================================

    val match_rdd = sqlContext.read.parquet("/user/cheil/" + input + "/match/").rdd
      .filter(_.getAs[String]("userId") == "a*98:F1:70:03:4C:29$FC:19:10:2F:B7:79$e003c64a34a85430$$355848064596409$1349")
      .map { line => (line.getAs[String]("appStoreLogId"), line.getAs[String]("userId"))
    }.groupByKey.map{case (x, y) => (x, y.head)}

   println(match_rdd.first()+"============="+match_rdd.count())

    val action_rdd = sqlContext.read.parquet("/user/cheil/"+input+"/action/").rdd.filter(line => line.getAs[Int]("actionType") == 3)
      .map{line =>
        if(line.getAs[String]("referLogId") == "54273987-34da-458f-b806-3f6e52d380de")  println(line+"===============------------------------")
        ((line.getAs[String]("referLogId"), line.getAs[Int]("actionType")), line.getAs[Long]("appId"))}
      .groupByKey(20).map{case (x, y) =>
      // referLogId, appId
      (x._1, y.head)
    }
    println(action_rdd.count() +"---------------"+ action_rdd.first())

     val join1 =  action_rdd.join(match_rdd)

   println(join1.count()+"-=-=-=-=-=-=-=-=-"+join1.first())

       join1.map{case (k, (a, m)) =>  // appid, userid
         (m, a)
    }.groupByKey(50)
         .filter(_._2.size > 1)
         // .filter(x => x._2.toSet.size < x._2.size )
     .map{
      case(k, v) =>  (k, v.size, v.toSet.size, v.toSet.mkString("|"))
    }.saveAsTextFile(output)

    /**

    //================================================================
    /**
      * 处理匹配数据
      * 1. 读取parquet文件
      * 2. 以appStoreLogId为key, 提取待统计数据(包括维度信息和统计信息)
      * 3. groupByKey  数据去重
      */
    val match_rdd = sqlContext.read.parquet("/user/cheil/" + input + "/match/").rdd
    // .filter{line =>
    //  line.getAs[Long]("campaignId") == 6097  &&
    //  line.getAs[Long]("strategyId") == 19739 &&
    //   line.getAs[Long]("creativeId") == 14384
    //  }
   // println(match_rdd.count() + "  mnatch 数据量")

    val match_rdd_group = match_rdd.map { line =>
      (line.getAs[String]("appStoreLogId"), s"${line.getAs[String]("userId")}--${line.getAs[String]("userIp")}")
    }.groupByKey(2000)

   // println(match_rdd_group.count() + "  mnatch 根据 appStoreLogId 去重后的数据量")


    val match2 = match_rdd_group.map {
      case (k, line) => (line.head, "")
    }.groupByKey.filter(_._2.size >= 10000) .map { case (x, y) => (x, y.size)}

    //match2.saveAsTextFile(output + "/match/")


    //==============================================================

   // match2.groupByKey.filter(_._2.sum >= 10000)
      .leftOuterJoin(action_rdd).map{
      case (k, (m, Some(a))) => s"${k} : ${m} ,  下载数: ${a(0)}, 安装数: ${a(1)} }"
      case (k, (m, None)) => s"${k} : ${m} ,  下载数: 0, 安装数: 0 }"
    }.saveAsTextFile(output)


    //.union(action_rdd).groupByKey(200)
    //.map{case(k, v) =>(k,  v.size)}.filter(_._2 >= 10000).map{case(k, v) => raw"${k}  :  ${v}"}

    // println(match_rdd.count() +"--------------")


    //    val all = impre_rdd.fullOuterJoin(match_rdd).map{
    //      case (key,(Some(a), Some(b))) => 1
    //      case (key,(Some(a), None)) => 2
    //      case (key,(None, Some(b))) => 3
    //    }.cache()
    //
    //
    //    println(all.filter(_ == 1).count()+"---match, impre 都存在")
    //    println(all.filter(_ == 2).count()+"---match 存在, impre 不存在")
    //    println(all.filter(_ == 3).count()+"---match 不存在, impre 存在")
    //
    //    println( match_rdd.join(impre_rdd).count()+"  ----- match, impre 都存在 --  join测试")

    // rdd2.saveAsTextFile(output)

*/
    sc.stop
  }


}
