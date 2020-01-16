package com.lx.supplement.run.batchBean


import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import com.lx.util.DateUtils
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

case class RcUser(readMysql:Boolean=false) extends RcBean {

  val sdf = DateUtils.getSdf("yyyy-MM-dd HH:mm:ss")

  private def rowToMap(row: Row) = {
    Map[String, String](
      "id" -> row.getAs[Int]("id").toString,
      "app_id" -> row.getAs[Int]("app_id").toString,
      "gender" -> row.getAs[Int]("gender").toString,
      "country_id" -> row.getAs[Int]("country_id").toString,
      "country_name" -> row.getAs[String]("country_name").toString,
      "gold_num" -> row.getAs[Double]("gold_num").toString,
      "platform_type" -> row.getAs[Int]("platform_type").toString,
      "pay_status" -> row.getAs[Int]("pay_status").toString,
      "status" -> row.getAs[Int]("status").toString,
      "stone_version" -> row.getAs[Int]("stone_version").toString,
      "eroticism_behavior" -> row.getAs[Int]("eroticism_behavior").toString,
      "sign_eroticism" -> row.getAs[Int]("sign_eroticism").toString,
      "channel" -> row.getAs[Int]("channel").toString,
      "type" -> row.getAs[Int]("type").toString,
      "create_time" -> (if(readMysql){sdf.format(row.getAs[Timestamp]("create_time"))} else {row.getAs[String]("create_time").toString}) ,
      "update_time" -> (if(readMysql){sdf.format(row.getAs[Timestamp]("update_time"))} else {row.getAs[String]("update_time").toString})
    )
  }


  override def rowToJson(row: Row): String = {
    val map = rowToMap(row)
    val data = JSON.toJSON(map.asJava)
    "{\"data\":[" + data + "],\"database\":\"rc_video_chat\",\"old\":null,\"table\":\"rc_user\",\"ts\":" + sdf.parse(map.get("update_time").get).getTime + ",\"type\":\"INSERT\"}"
  }

  override def mysqlTableName: String = "rc_user"
}