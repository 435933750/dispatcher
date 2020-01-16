package com.lx.monitor.run

import java.util.{Calendar, Date}

import com.lx.monitor.run.BatchComputeRate.theDayBeforeYesterday
import com.lx.util.DateUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object BatchComputeRate {

  /*case class Comp(var date: String="", var ttype: String="",var hours: Int=0, var hourValue: Double=0.0,var yesterdayHourValue: Double=0.0, var sevenDaysAgoHourValue: Double=0.0,
                  var dayValue: Double =0.0,var sevenDayValue: Double=0.0,
                  var yesterdayHourRate: Double =0.0,var sevendayHourRate: Double=0.0,var sevenDayRate: Double=0.0)*/

  case class Comp(var date: String = "", var ttype: String = "", var hourValue: Double = 0.0, var yesterdayHourValue: Double = 0.0, var sevenDaysAgoHourValue: Double = 0.0,
                  var yesterdayHourRate: Double = 0.0, var sevendayHourRate: Double = 0.0)


  val (yesterday, theDayBeforeYesterday, sevenDaysAgo) = getDateRange()
  val sales_detail = s"SELECT 'sales' as ttype,dt,DATE_FORMAT(create_time, 'yyyyMMddHH') cur_hour,money from rc_video_chat.rc_user_pay_record WHERE dt in ('${yesterday}','${theDayBeforeYesterday}','${sevenDaysAgo}')"
  val sales_date = s"SELECT ttype,dt,sum(money) sum_value from sales group by cur_date order by cur_date"
  val sales_hour = s"SELECT ttype,cur_hour dt,sum(money) sum_value from sales group by cur_hour order by cur_hour"

  val compMap = mutable.Map[String, mutable.Map[String, Comp]]()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BatchComputeRate").enableHiveSupport().getOrCreate()
    val sales = spark.sql(sales_detail).cache()
    sales.createOrReplaceTempView("sales")
    val salesDate = spark.sql(sales_date).collect().map(row => {
      val ttype = row.getAs[String]("ttype")
      val dt = row.getAs[String]("dt")
      val value = row.getAs[Double]("sum_value")
      (ttype, dt, value)
    })

    //    spark.sql(sales_hour).show()
    //    spark.sql(sales_date).write.mode(SaveMode.Overwrite).text("/tmp/sales_date")
    //    spark.sql(sales_hour).write.mode(SaveMode.Overwrite).text("/tmp/sales_hours")
    //    sales.unpersist()
  }

  def getCompareDate(dates: Array[(String, Double)]) = {
    val first = dates.head
    val last = dates.last
    if (last._2 >= first._2) {
      0.0
    } else {
      (first._2 - last._2) / first._2 * 100
    }
  }

  def getCompareHours(hours: Array[(String, Double)]) = {

    val yesterdays = hours.filter(_._1.startsWith(yesterday)).map(m => (m._1.takeRight(2), m._2)).toMap
    val theDayBeforeYesterdays = hours.filter(_._1.startsWith(theDayBeforeYesterday)).map(m => (m._1.takeRight(2), m._2)).toMap
    val sevenDaysAgos = hours.filter(_._1.startsWith(sevenDaysAgo)).map(m => (m._1.takeRight(2), m._2)).toMap
    var zip = yesterdays.zip(theDayBeforeYesterdays)


    val compute = (m: ((String, Double), (String, Double))) => {
      val left = m._1
      val right = m._2
      (yesterday, m._1._1, m._1._2, m._2._2, if (left._2 >= right._2) {
        0.0
      } else {
        (right._2 - left._2) / right._2 * 100
      })
    }


    val left = zip.map(compute)
    zip = yesterdays.zip(sevenDaysAgos)
    val right = zip.map(compute)

    println(left.mkString(","))
    println("=======================================")
    println(right.mkString(","))


  }

  //获取到昨天，前天，7天前的数据
  def getDateRange() = {
    val sdf = DateUtils.getSdf("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -1)
    val yesterday = sdf.format(cal.getTime)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    val theDayBeforeYesterdays = sdf.format(cal.getTime)
    cal.add(Calendar.DAY_OF_MONTH, -6)
    val sevenDaysAgo = sdf.format(cal.getTime)
    (yesterday, theDayBeforeYesterdays, sevenDaysAgo)
  }

}
