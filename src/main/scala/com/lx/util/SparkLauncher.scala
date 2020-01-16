package com.lx.util

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.LoggerFactory

/**
  * 合并线程
  */
object SparkLauncher {


  val logger = LoggerFactory.getLogger(SparkLauncher.getClass)
  val fs = FsUtils()

  def launcher(appName: String, mainClass: String, callBack: (SparkAppHandle) => Unit, params: String*): Unit = {

    params.foreach(println)
    val launcher = new SparkLauncher().
      setAppName(appName).
      setSparkHome(Constant.SPARK_HOME).
      setMaster("yarn").
      setConf("spark.driver.memory", "1g").
      setConf("spark.executor.memory", "10g").
      setConf("spark.executor.cores", "4").
      setConf("spark.executor.instances", "3").
      setConf("spark.dynamicAllocation.enabled", "false").
      setDeployMode("client").
      setAppResource(Constant.MERGE_JAR).
      setMainClass(mainClass).
      addAppArgs(params: _*)

    fs.list("/ssc/external_jars").foreach(path => {
      launcher.addJar(path.toUri.toString)
    })

    val handler = launcher.startApplication(new SparkAppHandle.Listener() {
      override def stateChanged(handle: SparkAppHandle): Unit = {
        logger.warn(s"sparkLauncher stateChanged:${handle.getAppId},state=${handle.getState}")
      }

      override def infoChanged(handle: SparkAppHandle): Unit = {
        logger.warn(s"sparkLauncher infoChanged:${handle.getAppId},state=${handle.getState}")
      }

    })

    while (!"FINISHED".equalsIgnoreCase(handler.getState.toString) && !"FAILED".equalsIgnoreCase(handler.getState.toString)) {
      //      logger.warn(s"sparkLauncher appId:${handler.getAppId},state=${handler.getState}")
      try
        Thread.sleep(10000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }
    callBack(handler)
  }

}
