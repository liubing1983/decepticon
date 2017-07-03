package com.lb.spark.appstore.daily

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import com.cheil.pengtai.appstore.pojo.log.AppStoreLog
import org.apache.spark.sql.types.StringType

/**
  * Created by samsung on 2017/6/19.
  */
object TestParquet {

  //spark-submit --master yarn-cluster   --class com.lb.spark.appstore.daily.TestParquet  spark-1.0-SNAPSHOT.jar  2017008   out-spark/1/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GenericLoadSave")//.setMaster("local")
    //System.setProperty("hadoop.home.dir", "D:\\hadoop")
    val sc = new JavaSparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //parquet 带表结构 ？？？
    //val usersDF = sqlContext.read.parquet("hdfs://appstore-stg-namenode1:8020/user/cheil/out-5/action/")

    println("+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")

   // val a = usersDF.rdd.map{ line =>
   //   line.mkString("---")
   // }

    val mode_parquet = sqlContext.read.parquet("hdfs://appstore-stg-namenode1:8020/user/cheil/out-5/action/")
    println(mode_parquet.schema+"=================================")

    mode_parquet.foreach{ line =>
      println((line.getAs[String]("appStoreLogId"),line.getAs[Long]("appId") ))
    }

    val a = mode_parquet.rdd.map{ line =>

      println(line+"--------------")
      println(line.mkString(":"))
     val b =  (line.getAs[String]("appStoreLogId"),line.getAs[Long]("appId") )
println(b)
      b
    }
    //mode_parquet.rdd.filter { row =>
   //   row.getAs[Long]("create_time")
   // }
  //}


    println(a.top(3)+"-----")
    println(a.count()+"====")
    println(a.first()+"----")
//    usersDF.registerTempTable("appstore")
//
//    val males = sqlContext.sql("select * from appstore ")
//    males.collect.foreach(println)


    //没有指定format  就是写入到磁盘的数据格式     默认是parquet
   // usersDF.select("name", "favorite_color").write.mode(SaveMode.Overwrite).save("hdfs://hadoop1:9000/output/namesAndFavColors_scala")
    //val pDF = sqlContext.read.parquet("hdfs://hadoop1:9000/output/namesAndFavColors_scala")
    //pDF.show()
  }

}
