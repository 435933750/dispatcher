package com.lx.monitor.task

import java.util._
import java.util.concurrent.TimeUnit

import com.lx.monitor.bean.CalculateTargetConfBean
import com.lx.monitor.run.StartMonitor._
import com.lx.monitor.task.ExecutorSql.logger
import com.lx.util.{Constant, SparkLauncher}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class ExecutorTask(spark: SparkSession) extends Thread {

  override def run(): Unit = {

    val prop = new Properties()
    prop.setProperty("user", Constant.JDBC_USER_NAME)
    prop.setProperty("password", Constant.JDBC_PASS_WORD)
    prop.setProperty("driver", Constant.JDBC_DRIVER)
    val client = new HttpClient
    while (true) {
      val beans = ArrayBuffer[CalculateTargetConfBean]()
      beans.append(ScheduleTask.runningTask.take())
      logger.warn("received task,start waiting...")
      Thread.sleep(60000)
      var bean: CalculateTargetConfBean = null
      do {
        bean = ScheduleTask.runningTask.poll(20, TimeUnit.SECONDS)
        if (bean != null) {
          beans.append(bean)
        }
      } while (bean != null)
      logger.warn(s"watining over,size:${beans.size}")

      val parser = beans.map(b => {
        val (start, end) = filterTime(b.monitorInterval)
        val startStr = sdf.format(start.getTime)
        val endStr = sdf.format(end.getTime)
        val hourStr = sdf_h.format(start.getTime)
        //        val sql = s"${b.sqlLogic.replace("|", s" and create_time >= '${startStr}' and create_time < '${endStr}' ")}"
        val sql = s"${b.sqlLogic.replace("$s_date", s"'${startStr}'")}"
          .replace("$e_date", s"'${endStr}'")
        logger.warn(s"ready executor id=${b.id} sql:${sql} saveTable:${b.saveTable}")
        (sql, b, hourStr)
      })

      val sqls = parser.map(p => p._1 + "|" + p._2.saveTable)
      val intervalGroup = parser.map(_._2).groupBy(_.monitorInterval)

      SparkLauncher.launcher("monitorSQL", ExecutorSql.getClass.getCanonicalName.dropRight(1), handler => {
        intervalGroup.foreach(i => {
          sendHttp(client, parser.head._3, i._1)
        })
      }, sqls: _*)
      logger.warn(s"end executor ids=${beans.map(_.id).mkString(",")}")
    }
  }

  import org.apache.commons.codec.binary.Base64

  def sendHttp(client: HttpClient, hour: String, interval: Int): Unit = {
    try {
      val msg = "The Alarm System is under the period of data verification, the following reports are for reference only, thanks."
      val base64Msg = new String((new Base64()).encode(msg.getBytes))
      val uri = s"http://10.0.6.123:9999/interval:${interval},dataDate:${hour},emailContent:${base64Msg}"
      logger.warn(s"sendHttp:${sdf.format(new Date())} : ${uri}")
      val method = new GetMethod(uri)
      client.executeMethod(method)
      method.releaseConnection()
    } catch {
      case e: Exception => logger.error("send http error", e)
    }

  }

  //时间条件
  def filterTime(interval: Int) = {
    val cla = Calendar.getInstance()
    val minute = cla.get(Calendar.MINUTE)
    interval match {
      case 30 => {
        if (minute < 30) {
          getBetween(cla, -1, 30, 0, 0)
        } else {
          getBetween(cla, 0, 0, 0, 30)
        }
      }
      case 60 => {
        getBetween(cla, -1, 0, 0, 0)
      }
    }
  }


  def getBetween(cla: Calendar, startHour: Int, startMinute: Int, endHour: Int, endMinute: Int) = {
    (new GregorianCalendar(cla.get(Calendar.YEAR),
      cla.get(Calendar.MONTH),
      cla.get(Calendar.DAY_OF_MONTH),
      cla.get(Calendar.HOUR_OF_DAY) + startHour, startMinute, 0),
      new GregorianCalendar(cla.get(Calendar.YEAR),
        cla.get(Calendar.MONTH),
        cla.get(Calendar.DAY_OF_MONTH),
        cla.get(Calendar.HOUR_OF_DAY) + endHour, endMinute, 0))
  }

}
