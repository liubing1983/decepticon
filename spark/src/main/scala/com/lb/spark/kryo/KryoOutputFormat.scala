package com.lb.spark.kryo

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

/**
  * Created by liub on 2016/12/21.
  */
class KryoOutputFormat(val path : String) {

  def saveAsTestFile4Kryo[T: ClassTag](rdd: RDD[T]): Unit ={
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)


  }

}
