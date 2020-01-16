package com.lx.config

import org.aeonbits.owner.Config
import org.aeonbits.owner.Config.{DefaultValue, Key}

@Config.Sources(Array("classpath:supplement/prod.properties"))
//@Config.Sources(Array("classpath:supplement/test.properties"))
trait SupplementConfig extends Config with Serializable {

  @Key("FM")
  @DefaultValue("c1")
  var FM: String = _

  @Key("BASE_PATH")
  @DefaultValue("/ssc/c2/")
  var BASE_PATH: String = _

  @Key("ERROR_PATH")
  @DefaultValue("/ssc/c2/error")
  var ERROR_PATH: String = _

  @Key("STOP_PATH")
  @DefaultValue("/ssc/c2/stop")
  var STOP_PATH: String = _


  @Key("GROUP_ID")
  @DefaultValue("liji")
  var GROUP_ID: String = _

  @Key("BOOTSTRAP_SERVERS")
  @DefaultValue("master:9092,slave1:9092,slave2:9092")
  var BOOTSTRAP_SERVERS: String = _

  @Key("ZK_SERVERS")
  @DefaultValue("master:2181,slave1:2181,slave2:2181")
  var ZK_SERVERS: String = _

  @Key("TOPICS")
  @DefaultValue("canal_user,canal_stat,canal_pay,canal_raffle")
  var TOPICS: String = _

  @Key("AUTO_OFFSET_RESET")
  @DefaultValue("largest")
  var AUTO_OFFSET_RESET: String = _

  @Key("BATCH_SIZE")
  @DefaultValue("10")
  var BATCH_SIZE: String = _

}
