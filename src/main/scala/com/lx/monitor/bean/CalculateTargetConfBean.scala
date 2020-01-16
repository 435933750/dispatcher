package com.lx.monitor.bean

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.lx.monitor.task.ScheduleTask


case class CalculateTargetConfBean(id: Int, var status: String, var target: String, var sqlLogic: String, var monitorInterval: Int, var saveTable: String) {

  private val lock = new ReentrantReadWriteLock()


  /**
    * 如果数据库读出来的配置有变化，则更新到最新的对象里
    * 如果interval更新了,则更新任务
    *
    * @param other
    */
  def hasChangedInterval(other: CalculateTargetConfBean) = {
    var has = false
    if (target != other.target) {
      target = other.target
    }
    if (sqlLogic != other.sqlLogic) {
      sqlLogic = other.sqlLogic
    }
    if (monitorInterval != other.monitorInterval) {
      monitorInterval = other.monitorInterval
      has = true
    }
    if (saveTable != other.saveTable) {
      saveTable = other.saveTable
    }
    has
  }

  def isDisable(other: CalculateTargetConfBean) = {
    var has = false
    if (other.status != ScheduleTask.active) {
      status = other.status
      has = true
    }
    has
  }
}
