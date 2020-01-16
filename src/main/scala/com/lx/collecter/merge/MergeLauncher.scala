package com.lx.collecter.merge

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Calendar, GregorianCalendar}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lx.util.{Constant, FsUtils, StringUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
  * 合并线程
  */
object MergeLauncher {


  val logger = LoggerFactory.getLogger(MergeLauncher.getClass)
  val fs = FsUtils()

  def main(args: Array[String]): Unit = {

    logger.warn("开始执行merge定时任务")
    val cla = Calendar.getInstance()
    val tommorowDate = new GregorianCalendar(cla.get(Calendar.YEAR),
      cla.get(Calendar.MONTH),
      cla.get(Calendar.DAY_OF_MONTH),
      cla.get(Calendar.HOUR_OF_DAY),
      cla.get(Calendar.MINUTE) + 1, 10)
//        val tommorowDate = new GregorianCalendar(cla.get(Calendar.YEAR),
//          cla.get(Calendar.MONTH),
//          cla.get(Calendar.DAY_OF_MONTH),
//          cla.get(Calendar.HOUR_OF_DAY) + 1, 10, 0)
    val lock = new ReentrantLock()
    val task = new Runnable {
      override def run(): Unit = {
        try {
          if (lock.tryLock()) {
            logger.warn("merge开始")
            start()
          }
        } finally {
          if (lock.isLocked) {
            lock.unlock()
          }
        }
      }
    }
    val exe = Executors.newSingleThreadScheduledExecutor()
    exe.scheduleAtFixedRate(task, (tommorowDate.getTimeInMillis() - cla.getTimeInMillis()), 6000, TimeUnit.MILLISECONDS)
  }

  def start() {
    val now = StringUtils.timestamp2String(System.currentTimeMillis(), "yyyyMMddHH00").toLong
    logger.warn("start merge,time is " + now)
    try {
      //查询小于当前小时之前的所有未合并的完成文件
      val files = fs.list(Constant.UNMERGE_PATH, path => {
        try {
          FileName.getTime(path.getName).toLong < now
        } catch {
          case e: Exception => logger.error(s"find merge file error:${path}", e); false
        }
      })
      //按小时分组
      val fileGroup = groupByHour(files)
      if (fileGroup.isEmpty) {
        logger.warn("no data need merge")
        return
      }

      val param = new java.util.HashMap[String, java.util.List[String]]()
      fileGroup.foreach(f => {
        param.put(f._1, f._2.asJava)
      })
      val json = (JSON.toJSON(param)).asInstanceOf[JSONObject].toJSONString
      launcher(json, Merger.getClass.getName.dropRight(1))

      logger.warn("merge success " + now)
    } catch {
      case e: Exception => {
        logger.error("merge error", e)
      }
    }
  }


  /**
    *
    * @param files
    * @return (合并后的存放路径,需要合并到当前路径下的所有文件)
    */
  def groupByHour(files: ArrayBuffer[Path]): mutable.Map[String, ArrayBuffer[String]] = {
    val complateFile = mutable.Map[String, ArrayBuffer[String]]()
    files.foreach(file => {
      val path = file.toUri.getPath
      if (file.toString.takeRight(Constant.COMPLATE_SUFFIX.size) == Constant.COMPLATE_SUFFIX) {
        val storagePath = path.splitAt(path.lastIndexOf("/") + 1)._1.replace(Constant.UNMERGE_PATH, Constant.MERGE_BY_HOUR)
        val time = FileName.getTime(file.getName)
        val key = if (time.size >= 10) {
          val year = time.slice(0, 4)
          val month = time.slice(4, 6)
          val day = time.slice(6, 8)
          val hour = time.slice(8, 10)
          storagePath + year + "/" + month + "/" + day + "/" + hour
        } else {
          Constant.MERGED_PATH + "other"
        }
        val array = complateFile.getOrElse(key, ArrayBuffer())
        array += path
        complateFile.put(key, array)
      }
    })
    complateFile
  }


  def launcher(param: String, mainClass: String): Unit = {
    val handler = new SparkLauncher().setAppName("MergeSparkLauncher").setSparkHome(Constant.SPARK_HOME).setMaster("yarn").
      setConf("spark.driver.memory", "1g").setConf("spark.executor.memory", "2g").
      setConf("spark.executor.cores", "3").
      setAppResource(Constant.MERGE_JAR).setMainClass(mainClass).
      addAppArgs(param).
      setDeployMode("cluster").
      startApplication(new SparkAppHandle.Listener() {

        override def stateChanged(handle: SparkAppHandle): Unit = {
          System.out.println("**********  state  changed  **********")
        }

        override def infoChanged(handle: SparkAppHandle): Unit = {
          System.out.println("**********  info  changed  **********")
        }
      })

    while ( {
      !"FINISHED".equalsIgnoreCase(handler.getState.toString) && !"FAILED".equalsIgnoreCase(handler.getState.toString)
    }) {
      System.out.println("id    " + handler.getAppId)
      System.out.println("state " + handler.getState)
      try
        Thread.sleep(10000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }
  }

}
