package com.lx.monitor.task

import java.util.TimerTask

import com.lx.monitor.bean.CalculateTargetConfBean
import com.lx.util.MysqlUtil
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 扫描mysql配置任务
  */
case class ScanMysqlConfigTask() extends TimerTask {
  val logger = LoggerFactory.getLogger(classOf[ScanMysqlConfigTask])


  override def run(): Unit = {
    try {
      logger.warn("开始扫描mysql")
      val conn = MysqlUtil.getConn
//      val rs = conn.prepareStatement("select * from data_service.alarm_calculate_target_conf").executeQuery()
      val rs = conn.prepareStatement("select * from data_service.alarm_calculate_target_conf").executeQuery()
      val configMap = mutable.Map[Int, CalculateTargetConfBean]()
      while (rs.next()) {
        val id = rs.getInt("id")
        configMap.put(id,
          CalculateTargetConfBean(
            id,
            rs.getString("status"),
            rs.getString("target"),
            rs.getString("sql_logic"),
            rs.getInt("monitor_interval"),
            rs.getString("save_table"))
        )
      }
      diffrent(configMap)
    } catch {
      case e: Exception => logger.error(s"scan mysql config error", e)
    }
  }

  private def diffrent(newMap: mutable.Map[Int, CalculateTargetConfBean]) {
    newMap.foreach(nm => {
      var task = ScheduleTask.get(nm._1)
      if (task == null && nm._2.status == ScheduleTask.active) {
        task = GenerateTask(nm._2)
        ScheduleTask.put(nm._1, task)
      } else if(task!=null){
        val disabled = task.config.isDisable(nm._2)
        if (disabled) {
          ScheduleTask.stop(task.id)
        } else {
          val changed = task.config.hasChangedInterval(nm._2)
          if (changed) {
            ScheduleTask.change(nm._1)
          }
        }
      }
    })
  }


}
