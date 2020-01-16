package com.lx.config

import org.aeonbits.owner.Config
import org.aeonbits.owner.Config.{DefaultValue, Key}

@Config.Sources(Array("classpath:jdbc.properties"))
trait JDBCConfig extends Config with Serializable {

  @Key("JDBC_URL")
  @DefaultValue("dbc:mysql://slave2:3306/test")
  var JDBC_URL: String = _

  @Key("JDBC_USER_NAME")
  @DefaultValue("root")
  var JDBC_USER_NAME: String = _

  @Key("JDBC_PASS_WORD")
  @DefaultValue("root")
  var JDBC_PASS_WORD: String = _

  @Key("JDBC_DRIVER")
  @DefaultValue("com.mysql.jdbc.Driver")
  var JDBC_DRIVER: String = _


  @Key("JDBC_BUSINESS_URL")
  @DefaultValue("dbc:mysql://slave2:3306")
  var JDBC_BUSINESS_URL: String = _

  @Key("JDBC_BUSINESS_USER_NAME")
  @DefaultValue("root")
  var JDBC_BUSINESS_USER_NAME: String = _

  @Key("JDBC_BUSINESS_PASS_WORD")
  @DefaultValue("root")
  var JDBC_BUSINESS_PASS_WORD: String = _

  @Key("JDBC_BUSINESS_DRIVER")
  @DefaultValue("com.mysql.jdbc.Driver")
  var JDBC_BUSINESS_DRIVER: String = _


}
