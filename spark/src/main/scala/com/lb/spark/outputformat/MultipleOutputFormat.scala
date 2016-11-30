package com.lb.spark.outputformat

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark._


// spark-submit --master spark://cloud138:7077 --executor-memory 10g  --class com.lb.spark.outputformat.MultipleOutputFormat  spark-1.0-SNAPSHOT.jar  liubing

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}

/**
 * 将输出文件按key输出
 * Created by liubing on 16-10-27.
 */
object MultipleOutputFormat {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MultipleOutputFormat")


    val sc = new SparkContext(sparkConf)
    sc.parallelize(List(("liubing", 100), ("lb", 100), ("liubing1", 100), ("lb1", 100))).partitionBy(new HashPartitioner(3))
      .saveAsHadoopFile(args(0), classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    sc.stop()
  }


}
