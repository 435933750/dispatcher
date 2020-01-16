package com.lx.supplement.run.batchBean

object RcFactory {

  def getBean(tableName: String,readMysql:Boolean=false): RcBean = {
    tableName match {
      case "rc_video_chat.rc_user" => RcUser(readMysql)
      case "rc_video_chat.rc_temp_user" => RcTempUser(readMysql)
      case "rc_video_chat.rc_goddess" => RcGoddess(readMysql)
      case "rc_video_chat.rc_user_pay_record" => RcUserPayRecord(readMysql)
      case "rc_live_chat_statistics.rc_user_praise_record" => RcUserPraiseRecord(readMysql)
      case _ => null
    }

  }
}
