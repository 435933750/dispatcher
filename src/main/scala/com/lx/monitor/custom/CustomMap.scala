package com.lx.monitor.custom

import com.lx.monitor.bean.ResetValueBean
import com.lx.util.{Constant, DateUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object CustomMap {

  val logger = LoggerFactory.getLogger(CustomMap.getClass)

  //createTime和RegisterTime是同一天，则返回YES,否则NO
  val compareTime = (leftCol: String, rightCol: String, patten: String) => (fieldMap: Map[String, ResetValueBean]) => {
    try {
      val ct = fieldMap.get(leftCol)
      val st = fieldMap.get(rightCol)
      val sdf = DateUtils.getSdf(patten)
      val rs = if (!ct.isEmpty && !st.isEmpty) {
        val d1 = sdf.parse(ct.get.value)
        val d2 = sdf.parse(st.get.value)
        d1.getTime == d2.getTime
      } else false
      if (rs) "YES" else "NO"
    } catch {
      case e: Exception => logger.error(s"compareTime error:${e.getMessage}"); "NO"
    }
  }

  //值映射
  val valueMapping = (colName: String, mapping: Map[String, String]) => (fieldMap: Map[String, ResetValueBean]) => {
    var newValue = ""
    try {
      val col = fieldMap.get(colName)
      col match {
        case Some(x1) => {
          newValue = x1.value
          mapping.get(x1.value) match {
            case Some(x2) => newValue = x2
            case None =>
          }
        }
        case None =>
      }
    } catch {
      case e: Exception => logger.error(s"createTimeCompareRegisterTime error:", e);
    }
    newValue
  }


  //9金币
  val nineMapping = (fieldMap: Map[String, ResetValueBean]) => {
    val rt = fieldMap.get("request_type")
    val gl = fieldMap.get("goddess_location")
    val ip = fieldMap.get("is_pay")
    val gv = fieldMap.get("goddess_video")
    if (!rt.isEmpty && !gl.isEmpty && !ip.isEmpty && !gv.isEmpty
      && rt.get.value == "0" && gl.get.value == "2" && ip == "1" && gv == "1") "9" else "0"
  }

  //业务的数据标识列表
  val alarmServiceListMapping = (fieldMap: Map[String, ResetValueBean]) => {
    val rt = fieldMap.get("request_type")
    val gl = fieldMap.get("goddess_location")
    val ip = fieldMap.get("is_pay")
    val gv = fieldMap.get("goddess_video")
    if (!rt.isEmpty && !gl.isEmpty && !ip.isEmpty && !gv.isEmpty
      && rt.get.value == "0" && gl.get.value == "2" && ip == "1" && gv == "1")
      "is_cost,is_match_connect,is_video_connect"
    else
      "is_match_connect,is_video_connect"
  }

  val actionSourceMapping = (fieldMap: Map[String, ResetValueBean]) => {
    val rt = fieldMap.get("request_type")
    val gl = fieldMap.get("goddess_location")
    val ip = fieldMap.get("is_pay")
    val gv = fieldMap.get("goddess_video")
    if (!rt.isEmpty && !gl.isEmpty && !ip.isEmpty && !gv.isEmpty
      && rt.get.value == "0" && gl.get.value == "2" && ip == "1" && gv == "1") "match"
    else {
      gl match {
        case Some(x) => x.value
        case None => Constant.MONITOR_NULL
      }
    }
  }


  val resetPk = () => (fieldMap: Map[String, ResetValueBean]) => {
    fieldMap.head._2.pk
  }


  private val tableMap = mutable.Map[String, mutable.Map[String, Map[String, ResetValueBean] => String]]()

  //公共属性
  val common = Map("1" -> "YES", "0" -> "NO")
  val gender = Map("1" -> "male", "2" -> "female")
  val vip = Map("0" -> "FREE", "3" -> "A", "2" -> "B", "1" -> "C")
  val isReal = Map("false" -> "NO", "true" -> "yes")

  //特殊属性
  val friend1 = Map("1" -> "YES", "2" -> "NO")
  val friend2 = Map("2" -> "YES", "1" -> "NO")
  val newUserType = Map("1" -> "NO", "0" -> "YES")

  var fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  tableMap.put("rc_user_pay_record", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", valueMapping("new_user_flg", common))
  tableMap.put("rc_user_record", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("remote_frist_pay_flg", compareTime("create_time", "remote_frist_pay_flg", "yyyy-MM-dd"))
  tableMap.put("rc_new_user_gift_detail", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("is_friend", valueMapping("is_friend", friend1))
  fieldMap.put("remote_frist_pay_flg", compareTime("create_time", "remote_frist_pay_flg", "yyyy-MM-dd"))
  fieldMap.put("sign_eroticism", valueMapping("sign_eroticism", isReal))
  tableMap.put("rc_goddess_goldnum_statistics", fieldMap)


  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("is_friend", valueMapping("is_friend", friend1))
  fieldMap.put("remote_frist_pay_flg", compareTime("create_time", "remote_frist_pay_flg", "yyyy-MM-dd"))
  fieldMap.put("sign_eroticism", valueMapping("sign_eroticism", isReal))
  tableMap.put("rc_minute_goldnum_record", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("vip", valueMapping("vip", vip))
  fieldMap.put("sign_eroticism", valueMapping("sign_eroticism", isReal))
  tableMap.put("rc_report_record", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  tableMap.put("rc_user_unblock_record", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", valueMapping("new_user_flg", common))
  tableMap.put("rc_video_snapshots_even", fieldMap)
  tableMap.put("rc_video_snapshots_odd", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("sign_eroticism", valueMapping("sign_eroticism", isReal))
  fieldMap.put("user_id", resetPk())
  tableMap.put("rc_user", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", valueMapping("comm", common))
  tableMap.put("rc_match_request_even", fieldMap)
  tableMap.put("rc_match_request_odd", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("remote_new_user_flg", compareTime("create_time", "remote_register_time", "yyyy-MM-dd"))
  fieldMap.put("remote_frist_pay_flg", compareTime("create_time", "remote_frist_pay_flg", "yyyy-MM-dd"))
  fieldMap.put("match_user_pay", valueMapping("match_user_pay", common))
  tableMap.put("rc_match_stat_even", fieldMap)
  tableMap.put("rc_match_stat_odd", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("remote_frist_pay_flg", compareTime("create_time", "remote_frist_pay_flg", "yyyy-MM-dd"))
  fieldMap.put("gold_num", nineMapping)
  fieldMap.put("action_source", actionSourceMapping)
  fieldMap.put("alarm_service_list", alarmServiceListMapping)
  fieldMap.put("is_friend", valueMapping("is_friend", friend1))
  fieldMap.put("match_user_type", valueMapping("match_user_type", newUserType))
  fieldMap.put("match_user_pay", valueMapping("match_user_pay", common))
  fieldMap.put("sign_eroticism", valueMapping("sign_eroticism", isReal))
  tableMap.put("rc_video_record_even", fieldMap)
  tableMap.put("rc_video_record_odd", fieldMap)

  fieldMap = mutable.Map.empty[String, Map[String, ResetValueBean] => String]
  fieldMap.put("new_user_flg", compareTime("create_time", "register_time", "yyyy-MM-dd"))
  fieldMap.put("remote_new_user_flg", compareTime("create_time", "remote_register_time", "yyyy-MM-dd"))
  fieldMap.put("remote_frist_pay_flg", compareTime("create_time", "frist_pay_flg", "yyyy-MM-dd"))
  tableMap.put("rc_friend_request_record", fieldMap)

  //给每个表设置通用规则
  tableMap.foreach(map => {
    map._2.put("gender", valueMapping("gender", gender))
    map._2.put("pay_status", valueMapping("pay_status", common))
    map._2.put("frist_pay_flg", compareTime("create_time", "frist_pay_flg", "yyyy-MM-dd"))
    map._2.put("eroticism_behavior", valueMapping("eroticism_behavior", common))
    map._2.put("sign_eroticism", valueMapping("sign_eroticism", common))
    map._2.put("remote_gender", valueMapping("gender", gender))
    map._2.put("remote_eroticism_behavior", valueMapping("remote_eroticism_behavior", common))
    map._2.put("remote_sign_eroticism", valueMapping("remote_sign_eroticism", common))
    map._2.put("is_real_user", valueMapping("is_real_user", common))
  })

  def get(table: String, field: String): Option[Map[String, ResetValueBean] => String] = {
    tableMap.get(table) match {
      case Some(x) => x.get(field) match {
        case Some(value) => {
          Some(value)
        }
        case None => None
      }
      case None => None
    }
  }


}
