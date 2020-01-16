package com.lx.supplement.run.batchBean

import java.sql.Timestamp
import java.util.Date

import com.alibaba.fastjson.JSON
import com.lx.util.DateUtils
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

case class RcTempUser(readMysql:Boolean=false) extends RcBean{

  val sdf = DateUtils.getSdf("yyyy-MM-dd HH:mm:ss")

  private def rowToMap(row: Row) = {
    Map[String, String](
      "user_id" -> row.getAs[Int]("user_id").toString,
    "create_time" -> (if(readMysql){sdf.format(row.getAs[Timestamp]("create_date"))} else {row.getAs[String]("create_date").toString})
    )
  }

  override def rowToJson(row: Row): String = {
    val map = rowToMap(row)
    val data = JSON.toJSON(map.asJava)
    "{\"data\":[" + data + "],\"database\":\"rc_video_chat\",\"old\":null,\"table\":\"rc_temp_user\",\"ts\":" + sdf.parse(map.get("create_time").get+" 00:00:00").getTime + ",\"type\":\"INSERT\"}"
  }

  override def mysqlTableName: String = "rc_temp_user"

  override def mysqlDBName: String = "rc_live_chat_statistics"

  override def JDBC_URL: String = "jdbc:mysql://10.0.5.72:3306/"

  override def filterCol: String = "create_date"
}