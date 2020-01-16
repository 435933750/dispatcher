package com.lx.monitor.task


import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.{Calendar, GregorianCalendar, Timer}

import com.lx.monitor.bean.CalculateTargetConfBean
import com.lx.util.DateUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object ScheduleTask {

  val logger = LoggerFactory.getLogger(ScheduleTask.getClass)
  val sdf = DateUtils.getSdf("yyyy-MM-dd HH:mm:ss")
  val seconds = (s: Int) => s * 1000L
  val minutes = (s: Int) => s * 1000L * 60
  val hours = (s: Int) => s * 1000L * 60 * 24
  val active = "online"


  private val tasks = new ConcurrentHashMap[Int, GenerateTask]()
  private val timer = new Timer()
  val runningTask = new LinkedBlockingQueue[CalculateTargetConfBean]()

  def startScan(spark: SparkSession): Unit = {
    val mysqlTask = ScanMysqlConfigTask()
    timer.schedule(mysqlTask, 0, seconds(60))
    new ExecutorTask(spark).start()
  }


  def put(id: Int, task: GenerateTask): Unit = {
    tasks.put(id, task)
    val cla = Calendar.getInstance()
    val minute = cla.get(Calendar.MINUTE)
    val nextTime = if (task.config.monitorInterval == 30) {
      if (minute < 30) {
        new GregorianCalendar(cla.get(Calendar.YEAR),
          cla.get(Calendar.MONTH),
          cla.get(Calendar.DAY_OF_MONTH),
          cla.get(Calendar.HOUR_OF_DAY),
          35, 0).getTimeInMillis
      } else {
        new GregorianCalendar(cla.get(Calendar.YEAR),
          cla.get(Calendar.MONTH),
          cla.get(Calendar.DAY_OF_MONTH),
          cla.get(Calendar.HOUR_OF_DAY) + 1,
          5, 0).getTimeInMillis
      }
    } else {
      new GregorianCalendar(cla.get(Calendar.YEAR),
        cla.get(Calendar.MONTH),
        cla.get(Calendar.DAY_OF_MONTH),
        cla.get(Calendar.HOUR_OF_DAY) + 1,
        5, 0).getTimeInMillis
    }
    logger.warn(s"任务${task.config.id}下次执行时间为:${sdf.format(nextTime)}")
    //todo 先执行一次,test
//    task.run()
    timer.schedule(task, nextTime - System.currentTimeMillis(), minutes(task.config.monitorInterval))
    //    timer.schedule(task, 0, seconds(task.config.monitorInterval))
  }

  def get(id: Int): GenerateTask = {
    tasks.get(id)
  }

  def change(id: Int): Unit = {
    val task = tasks.get(id)
    if (task != null) {
      task.reset(task.config.monitorInterval)
    }
  }

  def stop(id: Int): Unit = {
    val task = tasks.get(id)
    if (task != null) {
      task.stop()
      tasks.remove(id)
    }
  }


}
