package com.lx

import com.lx.util.FsUtils
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

/**
  * 通过HDFS检查目录来优雅关闭ssc
  *
  * @param stopFilePath
  * @param ssc
  */
case class RuntimeContext(stopFilePath: String, ssc: StreamingContext) {

  val logger = LoggerFactory.getLogger(RuntimeContext.getClass)

  val fs = FsUtils()
  //启动后执行
  //删除停止标记目录
  fs.delete(stopFilePath)

  def checkMarker = {
    fs.exists(stopFilePath)
  }

  var isStop = false

  def start(): Unit = {
    ssc.start()
    do {
      isStop = checkMarker || ssc.awaitTerminationOrTimeout(5000)
//      logger.warn(s"check stop mark:${isStop}")
    } while (!isStop)
    logger.warn("checked stop is true,starting stop the programmer")
    ssc.stop(true, true)
    logger.warn("stop successfully")
    System.exit(0)
  }

  def stop = {
    ssc.stop(true, false)
    System.exit(0)
  }


}
