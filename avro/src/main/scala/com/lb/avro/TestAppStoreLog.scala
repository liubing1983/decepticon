package com.lb.avro

import java.io.{File, IOException, InputStream}

import com.cheil.pengtai.appstore.pojo.log.AppStoreLog
import org.apache.avro.file.DataFileReader
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificDatumReader

/**
  * Created by samsung on 2017/5/4.
  */
object TestAppStoreLog {
  def main(args: Array[String]): Unit = {

    // 反序列化
    val reader: DatumReader[AppStoreLog] = new SpecificDatumReader();

    try {
      val fileReader: DataFileReader[AppStoreLog] = new DataFileReader[AppStoreLog](new File("e://123.log"), reader)
      println(fileReader.getSchema)
      while (fileReader.hasNext() ) {
        println(fileReader.next())
      }
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }
}
