package com.lx.config

import org.aeonbits.owner.Config
import org.aeonbits.owner.Config.{DefaultValue, Key}

@Config.Sources(Array("classpath:collecter/prod.properties"))
//@Config.Sources(Array("classpath:collecter/test.properties"))
trait CollecterConfig extends Config with Serializable {

  @Key("BATCH_SIZE")
  @DefaultValue("10")
  var BATCH_SIZE: String = _

  @Key("GROUP_ID")
  @DefaultValue("liji")
  var GROUP_ID: String = _

  @Key("HDFS")
  @DefaultValue("hdfs://master:9000")
  var HDFS: String = _

  @Key("BOOTSTRAP_SERVERS")
  @DefaultValue("master:9092,slave1:9092,slave2:9092")
  var BOOTSTRAP_SERVERS: String = _

  @Key("ZK_SERVERS")
  @DefaultValue("master:2181,slave1:2181,slave2:2181")
  var ZK_SERVERS: String = _

  @Key("TOPICS")
  @DefaultValue("ssc")
  var TOPICS: String = _

  @Key("BASE_PATH")
  @DefaultValue("/ssc/c1/")
  var BASE_PATH: String = _

  @Key("TMP_SUFFIX")
  @DefaultValue(".writing")
  var TMP_SUFFIX: String = _

  @Key("COMPLATE_SUFFIX")
  @DefaultValue(".cmp")
  var COMPLATE_SUFFIX: String = _

  @Key("AUTO_OFFSET_RESET")
  @DefaultValue("largest")
  var AUTO_OFFSET_RESET: String = _

  @Key("ZK_ROOT")
  @DefaultValue("/ssc/offset/")
  var ZK_ROOT: String = _

  @Key("HDFS_USER")
  @DefaultValue("hadoop")
  var HDFS_USER: String = _

  @Key("MERGE_JAR")
  @DefaultValue("/home/hadoop/job-dir/liji/spark/dispather-1.0-SNAPSHOT-jar-with-dependencies.jar")
  var MERGE_JAR: String = _

  @Key("SPARK_HOME")
  @DefaultValue("/usr/lib/spark")
  var SPARK_HOME: String = _

  @Key("PRIVATEKEY_PATH")
  @DefaultValue("/home/hadoop/job-dir/liji/spark/private.der")
  var PRIVATEKEY_PATH: String = _

  @Key("UNMERGE_PATH")
  @DefaultValue("/ssc/c1/unmerge/")
  var UNMERGE_PATH: String = _

  @Key("MERGED_PATH")
  @DefaultValue("/ssc/c1/merged/")
  var MERGED_PATH: String = _

  @Key("MERGE_BY_HOUR")
  @DefaultValue("/ssc/c1/merged_by_hour/")
  var MERGE_BY_HOUR: String = _

  @Key("ERROR_PATH")
  @DefaultValue("/ssc/c1/error/")
  var ERROR_PATH: String = _

  @Key("STOP_PATH")
  @DefaultValue("/ssc/c1/stop")
  var STOP_PATH: String = _

}
