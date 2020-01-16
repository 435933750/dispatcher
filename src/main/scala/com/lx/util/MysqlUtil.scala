package com.lx.util

import java.sql.{Connection, DriverManager}

object MysqlUtil {

  @volatile private var conn: Connection = _

  val driver = Constant.JDBC_DRIVER
  val url = Constant.JDBC_URL
  val username = Constant.JDBC_USER_NAME
  val password = Constant.JDBC_PASS_WORD
  Class.forName(driver)

  def getConn: Connection = {
    if (conn == null || conn.isClosed()) {
      synchronized {
        if (conn == null || conn.isClosed()) {
          conn = DriverManager.getConnection(url, username, password)
        }
      }
    }
    conn
  }
}
