package com.lb.avro

import java.io.{File, IOException}

import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

/**
  * Created by samsung on 2017/4/22.
  */
object TestAvro {

  def main(args: Array[String]): Unit = {
    // 写入数据
    val user = new User("lb", 35, "hello")
    // 序列化
    val userDatumWriter: DatumWriter[User] = new SpecificDatumWriter()
    val dataFileWriter: DataFileWriter[User] = new DataFileWriter[User](userDatumWriter)
    try {
      // 创建文件
      dataFileWriter.create(user.getSchema(), new File("E:\\user.avro"));
      // 往文件中写入数据
      dataFileWriter.append(user);
      // 关闭流
      dataFileWriter.close();
    } catch {
      case e: IOException => e.printStackTrace()
    }

    //############################################################################################

    // 反序列化
    val reader: DatumReader[User] = new SpecificDatumReader();
    try {
      val fileReader: DataFileReader[User] = new DataFileReader[User](new File("E:\\user.avro"), reader)

      while (fileReader.hasNext()) {
        println(fileReader.next(user));
      }
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }
}
