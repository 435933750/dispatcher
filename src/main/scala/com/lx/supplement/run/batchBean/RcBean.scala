package com.lx.supplement.run.batchBean

import org.apache.spark.sql.Row

trait RcBean {
  def rowToJson(row: Row): String

  def mysqlTableName: String

  def mysqlDBName: String = "rc_video_chat"

  def filterCol = "create_time"

  def JDBC_URL = "jdbc:mysql://10.0.5.170:3306/"

  def JDBC_USER_NAME = "rc_read"

  def JDBC_PASS_WORD = "d3d123d+YfNQNVdVFr"
}
