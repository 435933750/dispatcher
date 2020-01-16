package com.lx.monitor.task

import java.util.Properties

import com.lx.util.Constant
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object ExecutorSql {

  val logger = LoggerFactory.getLogger(ExecutorSql.getClass)

  def main(args: Array[String]): Unit = {

    if (args == null || args.size < 1) {
      logger.error("execute sql is null")
    }
    logger.warn(s"start launch spark:${args.mkString(",")}")


    val prop = new Properties()
    prop.setProperty("user", Constant.JDBC_USER_NAME)
    prop.setProperty("password", Constant.JDBC_PASS_WORD)
    prop.setProperty("driver", Constant.JDBC_DRIVER)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val tulp = args.map(a => {
      val splits = a.split("\\|")
      (splits(0), splits(1))
    })

    tulp.foreach({ case (sql, saveTable) => {
      try {
        spark.sql(sql).write.mode(SaveMode.Append).jdbc(Constant.JDBC_URL, saveTable, prop)
      } catch {
        case e: Exception => e.printStackTrace()
      }
      logger.warn(s"end launch spark")
    }
    })
  }

  /*def main(args: Array[String]): Unit = {
    val sql = "SELECT create_date,platform_type,app_id,country_name,gender,new_user_flg,pay_status,frist_pay_flg,eroticism_behavior,channel,TYPE,VERSION,vip,sign_eroticism,role,tolerance,sexRate,STATUS,pay_platform,SUM(money) AS sales,COUNT(DISTINCT user_id) AS sales_uv,SUM(money)/COUNT(DISTINCT user_id) AS arpu FROM real_time_mysql_data.alarm_scheme_minute WHERE alarm_service_list='is_sales'  and create_time between '2020-01-03 17:00:00' and '2020-01-03 18:00:00'  GROUP BY create_date,platform_type,app_id,country_name,gender,new_user_flg,pay_status,frist_pay_flg,eroticism_behavior,channel,TYPE,VERSION,vip,sign_eroticism,role,tolerance,sexRate,STATUS,pay_platform"
    val prop = new Properties()
    prop.setProperty("user", Constant.JDBC_USER_NAME)
    prop.setProperty("password", Constant.JDBC_PASS_WORD)
    prop.setProperty("driver", Constant.JDBC_DRIVER)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate();
    val df = spark.sql(sql)
    df.show()

    df.write.mode(SaveMode.Append).jdbc(Constant.JDBC_URL, "alarm_sales", prop)
    logger.warn(s"end launch spark")
  }*/
}
