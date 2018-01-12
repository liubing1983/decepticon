package com.lb.spark.appstore.streaming


import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class abc(i1: Int, i2: Int)

/**
  * Created by samsung on 2017/5/25.
  */
object TestReport {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("report Demo")
    // 本地运行
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\")
    sparkConf.setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 设置检查点, 防止程序异常
    ssc.checkpoint(".")
    // 原始数据
    val lines = ssc.socketTextStream("localhost", 7777)

    // 实时报表统计
    lines.map { line =>
      line.split(",", -1) match {
        case Array(_, _, _) => ("report_ab", 1L)
        case Array("1", _, _, _) => ("report_bc", 1L)
        case _ => ("report_error", 1L)
      }
    }.updateStateByKey[Long](updateFunc).print

    // 保存原始数据
    // lines.saveAsTextFiles("123")

    // 启动监控
    ssc.start()
    // 启动后台线程, 等待作业完成, 并在n秒后自动结束任务
    ssc.awaitTerminationOrTimeout(1000 * 60 * 10)
  }

  val updateFunc = (values: Seq[Long], state: Option[Long]) => {
    Some(values.sum.toLong + state.getOrElse(0L))
  }

}
