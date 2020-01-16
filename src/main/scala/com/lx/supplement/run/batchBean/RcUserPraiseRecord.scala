package com.lx.supplement.run.batchBean

import java.sql.Timestamp
import java.util.Date

import com.alibaba.fastjson.JSON
import com.lx.util.DateUtils
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

case class RcUserPraiseRecord(readMysql:Boolean=false) extends RcBean {

  val sdf = DateUtils.getSdf("yyyy-MM-dd HH:mm:ss")

  private def rowToMap(row: Row) = {
    Map[String, String](
      "praise_user_id" -> row.getAs[Int]("praise_user_id").toString,
    "create_time" -> (if(readMysql){sdf.format(row.getAs[Timestamp]("create_time"))} else {row.getAs[String]("create_time").toString})
    )
  }

  override def rowToJson(row: Row): String = {
    val map = rowToMap(row)
    val data = JSON.toJSON(map.asJava)
    "{\"data\":[" + data + "],\"database\":\"rc_live_chat_statistics\",\"old\":null,\"table\":\"rc_user_praise_record\",\"ts\":" + sdf.parse(map.get("create_time").get).getTime + ",\"type\":\"INSERT\"}"
  }

  override def mysqlTableName: String = "rc_user_praise_record"

  override def mysqlDBName: String = "rc_live_chat_statistics"

  override def JDBC_URL: String = "jdbc:mysql://10.0.5.72:3306/"
}