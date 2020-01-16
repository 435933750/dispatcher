package com.lx.monitor.task

import java.util.TimerTask
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.lx.monitor.bean.CalculateTargetConfBean
import com.lx.util.DateUtils
import org.slf4j.LoggerFactory


/**
  * 执行sql任务
  *
  * @param interval
  * @param id
  */
case class GenerateTask(config: CalculateTargetConfBean) extends TimerTask {
  val logger = LoggerFactory.getLogger(GenerateTask.getClass)

  val id = config.id
  private var dbInterval = ScheduleTask.seconds(config.monitorInterval)
  private val rlock = new ReentrantReadWriteLock()
  val sdf = DateUtils.getSdf("HH:mm:ss")

  override def run(): Unit = {
    rlock.readLock().lock()
    if (config.status == ScheduleTask.active) {
      var interval = ScheduleTask.seconds(config.monitorInterval)
      if (interval != dbInterval) {
        rlock.readLock().unlock()
        rlock.writeLock().lock()
        if (interval != dbInterval) {
          dbInterval = interval
          resetPeriod(dbInterval)
        }
        rlock.writeLock().unlock()
      } else {
        rlock.readLock().unlock()
      }
      ScheduleTask.runningTask.put(config)
      logger.warn(config.status + "-" + config.saveTable + "-" + sdf.format(System.currentTimeMillis()))
    } else {
      rlock.readLock().unlock()
    }

  }

  private[task] def reset(millis: Long): Unit = {
    rlock.writeLock().lock()
    dbInterval = millis
    rlock.writeLock().unlock()
  }

  private[task] def stop(): Unit = {
    cancel
  }

  private def resetPeriod(time: Long): Unit = {
    val fields = this.getClass.getSuperclass.getDeclaredFields
    fields.foreach(field => {
      if (field.getName.endsWith("period")) {
        if (!field.isAccessible) {
          field.setAccessible(true)
        }
        field.set(this, time)
      }
    })
  }


}