package com.lx.util

import com.lx.config.{CollecterConfig, JDBCConfig, SupplementConfig}
import org.aeonbits.owner.ConfigFactory
import org.apache.hadoop.hbase.util.Bytes

object Constant {


  val collect = ConfigFactory.create(classOf[CollecterConfig])
  val supple = ConfigFactory.create(classOf[SupplementConfig])
  val jdbc = ConfigFactory.create(classOf[JDBCConfig])

  val BATCH_SIZE = collect.BATCH_SIZE
  val GROUP_ID = collect.GROUP_ID
  val HDFS = collect.HDFS
  val BOOTSTRAP_SERVERS = collect.BOOTSTRAP_SERVERS
  val ZK_SERVERS = collect.ZK_SERVERS
  val TOPICS = collect.TOPICS
  val BASE_PATH = collect.BASE_PATH
  val UNMERGE_PATH = collect.UNMERGE_PATH
  val MERGED_PATH = collect.MERGED_PATH
  val MERGE_BY_HOUR = collect.MERGE_BY_HOUR
  val TMP_SUFFIX = collect.TMP_SUFFIX
  val COMPLATE_SUFFIX = collect.COMPLATE_SUFFIX
  val AUTO_OFFSET_RESET = collect.AUTO_OFFSET_RESET
  val ZK_ROOT = collect.ZK_ROOT
  val ERROR_PATH = collect.ERROR_PATH
  val STOP_PATH = collect.STOP_PATH
  val HDFS_USER = collect.HDFS_USER
  val MERGE_JAR = collect.MERGE_JAR
  val SPARK_HOME = collect.SPARK_HOME
  val PRIVATEKEY_PATH = collect.PRIVATEKEY_PATH

  val JDBC_URL = jdbc.JDBC_URL
  val JDBC_USER_NAME = jdbc.JDBC_USER_NAME
  val JDBC_PASS_WORD = jdbc.JDBC_PASS_WORD
  val JDBC_DRIVER = jdbc.JDBC_DRIVER
  val JDBC_BUSINESS_URL = jdbc.JDBC_BUSINESS_URL
  val JDBC_BUSINESS_USER_NAME = jdbc.JDBC_BUSINESS_USER_NAME
  val JDBC_BUSINESS_PASS_WORD = jdbc.JDBC_BUSINESS_PASS_WORD
  val JDBC_BUSINESS_DRIVER = jdbc.JDBC_BUSINESS_DRIVER


  val SUPPLEMENT_ERROR_PATH = supple.ERROR_PATH
  val SUPPLEMENT_STOP_PATH = supple.STOP_PATH
  val SUPPLEMENT_GROUP_ID =supple.GROUP_ID
  val SUPPLEMENT_TOPICS =supple.TOPICS
  val SUPPLEMENT_ZK_SERVERS =supple.ZK_SERVERS
  val SUPPLEMENT_BOOTSTRAP_SERVERS =supple.BOOTSTRAP_SERVERS
  val SUPPLEMENT_AUTO_OFFSET_RESET =supple.AUTO_OFFSET_RESET
  val SUPPLEMENT_FM = Bytes.toBytes(supple.FM)
  val SUPPLEMENT_BATCH_SIZE = supple.BATCH_SIZE

  val MONITOR_UNKNOW_VALUE = "unknow"
  val MONITOR_NOSET_VALUE = "noset"
  val MONITOR_NULL = "novalue"

}
