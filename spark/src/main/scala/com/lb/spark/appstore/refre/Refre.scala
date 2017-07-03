package com.lb.spark.appstore.refre

import java.text.SimpleDateFormat

import com.cheil.pengtai.appstore.console.dp.analysis.day.refer.ReferCombineInputFormat
import com.cheil.pengtai.appstore.pojo.log.AppStoreLog
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by samsung on 2017/5/18.
  */
object Refre {

  // spark-submit --master yarn-cluster  --jars ./avro-1.0-SNAPSHOT.jar   --class com.lb.spark.appstore.refre.Refre  spark-1.0-SNAPSHOT.jar  hdfs://appstore-stg-namenode1:8020/user/cheil/appstore/match/20170513/02/1/*   out/3

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Refre Demo")

    val conf: Configuration = new Configuration()
    // 修改文件拆分最大大小
    conf.set("mapreduce.input.fileinputformat.split.minsize", "128000000")
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "128000000")

    val job = Job.getInstance(conf)
    AvroJob.setInputKeySchema(job, AppStoreLog.SCHEMA$)

    val sc = new SparkContext(sparkConf)

    val match_lines = sc.newAPIHadoopFile[AvroKey[AppStoreLog], NullWritable, AvroKeyInputFormat[AppStoreLog]](s"/appstore/flume/match/${args(0)}/*/*/*.log", classOf[AvroKeyInputFormat[AppStoreLog]], classOf[AvroKey[AppStoreLog]], classOf[NullWritable], conf)
    val impre_lines = sc.newAPIHadoopFile[AvroKey[AppStoreLog], NullWritable, AvroKeyInputFormat[AppStoreLog]](s"/appstore/flume/impre/${args(0)}/*/*/*.log", classOf[AvroKeyInputFormat[AppStoreLog]], classOf[AvroKey[AppStoreLog]], classOf[NullWritable], conf)
    val action_lines = sc.newAPIHadoopFile[AvroKey[AppStoreLog], NullWritable, AvroKeyInputFormat[AppStoreLog]](s"/appstore/flume/action/${args(0)}/*/*/*.log", classOf[AvroKeyInputFormat[AppStoreLog]], classOf[AvroKey[AppStoreLog]], classOf[NullWritable], conf)

    //val datumReader = new SpecificDatumReader[AppStoreLog](classOf[AppStoreLog])

    val match_rdd = match_lines.map { line =>

      val a : AppStoreLog  = line._1.datum()
      val logTime: String = sdf.format(a.getLogTimestamp()) //val dataFileReader = new DataFileReader[AppStoreLog](new SeekableByteArrayInput(line._2.copyBytes()), datumReader)

      // val b: List[AppStoreLog] = for(a: AppStoreLog <- dataFileReader.iterator() )yield {
      // val a = x
      //val data =  x._1.datum()
      a.setLogDate(logTime.substring(0, 10).trim)
      a.setLogTime(logTime.substring(10).trim)
      //"{a.getAppStoreLogId},${a.getBatchLogId},${a.getReferLogId},${a.getOriginAdsourceId},${a.getMatchOrder},${a.getAppId},${a.getSourcePorductId},${a.getSourceAppId},${a.getCreativeClickUrl},${a.getCampaignId},${a.getStrategyId},${a.getCreativeId},${m.getMediaId},${a.getAdPosId},${a.getStrategyPrice},${a.getDealPrice},${a.getPayPrice},${a.getEncodeAdsource},${a.getDecodeAdsource},${a.getLogType},${a.getMatchType},${a.getIsMatched},${a.getIsImpression},${a.getIsClick},${a.getActionType},${a.getIsHome},${a.getIsDownload},${a.getIsInstall},${a.getLogTimestamp},${m.getLogDate},${m.getLogTime},${m.getUserIp},${m.getZoneCountry},${m.getZoneProvince},${m.getZoneCity},${m.getUserId},${m.getUserPhone},${m.getUserAgent},${m.getUserOs},${m.getUserDeviceBrand},${m.getUserDeviceModel},${m.getUserLanguage},${m.getUserDisplaySize},${m.getUsernetWork},${a.getDeviceApps},${a.getSearchKeyWords},${a.getCheatType}
      (a.getAppStoreLogId.toString, a)
    }

    //    match_lines.map{ line =>
    //      val a = line._1.datum()
    //      s"""${a.getAppStoreLogId},${a.getBatchLogId},${a.getReferLogId},${a.getOriginAdsourceId},${a.getMatchOrder},${a.getAppId},${a.getSourcePorductId},${a.getSourceAppId},${a.getCreativeClickUrl},${a.getCampaignId},${a.getStrategyId},${a.getCreativeId},${a.getMediaId},${a.getAdPosId},${a.getStrategyPrice},${a.getDealPrice},${a.getPayPrice},${a.getEncodeAdsource},${a.getDecodeAdsource},${a.getLogType},${a.getMatchType},${a.getIsMatched},${a.getIsImpression},${a.getIsClick},${a.getActionType},${a.getIsHome},${a.getIsDownload},${a.getIsInstall},${a.getLogTimestamp},${a.getLogDate},${a.getLogTime},${a.getUserIp},${a.getZoneCountry},${a.getZoneProvince},${a.getZoneCity},${a.getUserId},${a.getUserPhone},${a.getUserAgent},${a.getUserOs},${a.getUserDeviceBrand},${a.getUserDeviceModel},${a.getUserLanguage},${a.getUserDisplaySize},${a.getUsernetWork},${a.getDeviceApps},${a.getSearchKeyWords},${a.getCheatType}"""
    //    }.repartition(20).saveAsTextFile(args(1))

    val match_rdd_group = match_rdd.groupByKey(50)

    // 合并点击日志
    action_lines.map { line =>
      (line._1.datum().getReferLogId.toString, line._1.datum())
    }.groupByKey(50).join(match_rdd_group).map {
      case (k, (v1, v2)) =>
        val a = v1.head
        val m = v2.head
        s"${a.getAppStoreLogId},${a.getBatchLogId},${a.getReferLogId},${a.getOriginAdsourceId},${a.getMatchOrder},${a.getAppId},${a.getSourcePorductId},${a.getSourceAppId},${a.getCreativeClickUrl},${a.getCampaignId},${a.getStrategyId},${a.getCreativeId},${m.getMediaId},${a.getAdPosId},${a.getStrategyPrice},${a.getDealPrice},${a.getPayPrice},${a.getEncodeAdsource},${a.getDecodeAdsource},${a.getLogType},${a.getMatchType},${a.getIsMatched},${a.getIsImpression},${a.getIsClick},${a.getActionType},${a.getIsHome},${a.getIsDownload},${a.getIsInstall},${a.getLogTimestamp},${m.getLogDate},${m.getLogTime},${m.getUserIp},${m.getZoneCountry},${m.getZoneProvince},${m.getZoneCity},${m.getUserId},${m.getUserPhone},${m.getUserAgent},${m.getUserOs},${m.getUserDeviceBrand},${m.getUserDeviceModel},${m.getUserLanguage},${m.getUserDisplaySize},${m.getUsernetWork},${a.getDeviceApps},${a.getSearchKeyWords},${a.getCheatType}"
    }.saveAsTextFile(s"${args(1)}/action")

    // 合并曝光日志
    //    impre_lines.map { line =>
    //      (line._1.datum().getReferLogId.toString, line._1.datum())
    //    }.groupByKey.join(match_rdd_group).map {
    //      case (k, (v1, v2)) =>
    //        val a = v1.head
    //        val m = v2.head
    //        s"${a.getAppStoreLogId},${a.getBatchLogId},${a.getReferLogId},${a.getOriginAdsourceId},${a.getMatchOrder},${a.getAppId},${a.getSourcePorductId},${a.getSourceAppId},${a.getCreativeClickUrl},${a.getCampaignId},${a.getStrategyId},${a.getCreativeId},${m.getMediaId},${a.getAdPosId},${a.getStrategyPrice},${a.getDealPrice},${a.getPayPrice},${a.getEncodeAdsource},${a.getDecodeAdsource},${a.getLogType},${a.getMatchType},${a.getIsMatched},${a.getIsImpression},${a.getIsClick},${a.getActionType},${a.getIsHome},${a.getIsDownload},${a.getIsInstall},${a.getLogTimestamp},${m.getLogDate},${m.getLogTime},${m.getUserIp},${m.getZoneCountry},${m.getZoneProvince},${m.getZoneCity},${m.getUserId},${m.getUserPhone},${m.getUserAgent},${m.getUserOs},${m.getUserDeviceBrand},${m.getUserDeviceModel},${m.getUserLanguage},${m.getUserDisplaySize},${m.getUsernetWork},${a.getDeviceApps},${a.getSearchKeyWords},${a.getCheatType}"
    //    }.saveAsTextFile(s"${args(1)}/impre")

    match_rdd.saveAsTextFile(s"${args(1)}/match")

    sc.stop()
  }

}
