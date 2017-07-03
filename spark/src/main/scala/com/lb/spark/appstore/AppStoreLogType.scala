package com.lb.spark.appstore

/**
  * Created by samsung on 2017/6/5.
  */
case class AppStoreLogType {
  // 日志类型常量：匹配// 日志类型常量：匹配

  final val  APP_LOG_TYPE_MATCH = 1

  // 日志类型常量-匹配：0:非匹配日志
  final  val APP_LOG_TYPE_MATCH_NO = 0
  // 日志类型常量-匹配：1:普通匹配
  final  val APP_LOG_TYPE_MATCH_COMMON = 1
  // 日志类型常量-匹配：2:搜索匹配
  final val APP_LOG_TYPE_MATCH_SEARCH = 2

  // 日志类型常量-匹配：未匹配成功
  val APP_LOG_TYPE_MATCH_SUCCESS_NO = 0
  // 日志类型常量-匹配：匹配成功
  val APP_LOG_TYPE_MATCH_SUCCESS_YES = 1

  // 日志类型常量：曝光
  val APP_LOG_TYPE_IMPRESSION = 2

  //广告曝光标志(0:无效值;)
  val APP_LOG_TYPE_IMPRESSION_INVALID = 0
  //广告曝光标志(1:已曝光)
  val APP_LOG_TYPE_IMPRESSION_VALID = 1

  // 日志类型常量：动作
  val APP_LOG_TYPE_ACTION = 3
  //动作类型(0:无效动作1:点击动作2:进入home动作3: 下载4:安装完成)
  val APP_LOG_TYPE_ACTION_TYPE_INVALID = 0
  //动作类型(1:点击动作)
  val APP_LOG_TYPE_ACTION_TYPE_CLICK = 1
  //动作类型(2:进入home动作)
  val APP_STORE_LOG_TYPE_ACTION_TYPE_HOME = 2
  //动作类型(3: 下载)
  val APP_LOG_TYPE_ACTION_TYPE_DOWNLOAD = 3
  //动作类型(4:安装完成)
  val APP_LOG_TYPE_ACTION_TYPE_INSTALL = 4

  //广告点击标志(0:无效值)
  val APP_LOG_TYPE_ACTION_CLICK_INVALID = 0
  //广告点击标志(1:已点击)
  val APP_LOG_TYPE_ACTION_CLICK_VALID = 1

  //广告进入主页面标志(0:无效值)
  val APP_LOG_TYPE_ACTION_HOME_INVALID = 0
  //广告进入主页面标志(1:已进入主页面)
  val APP_LOG_TYPE_ACTION_HOME_VALID = 1

  //广告下载标志(0:无效值)
  val APP_LOG_TYPE_ACTION_DOWNLOAD_INVALID = 0
  //广告下载标志(1:已下载)
  val APP_LOG_TYPE_ACTION_DOWNLOAD_VALID = 1

  //广告安装标志(0:无效值)
  val APP_LOG_TYPE_ACTION_INSTALL_INVALID = 0
  //广告安装标志(1:已安装)
  val APP_LOG_TYPE_ACTION_INSTALL_VALID = 1
}
