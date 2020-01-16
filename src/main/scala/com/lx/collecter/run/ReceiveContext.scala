package com.lx.collecter.run

import java.net.URI

import com.lx.RuntimeContext
import com.lx.util.Constant.HDFS
import com.lx.util.{Constant, FsUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

/**
  * 通过HDFS检查目录来优雅关闭ssc
  *
  * @param stopFilePath
  * @param ssc
  */
class ReceiveContext(override val stopFilePath: String, override val ssc: StreamingContext) extends RuntimeContext(stopFilePath,ssc) {

  override val logger = LoggerFactory.getLogger(classOf[ReceiveContext])

  //创建未合并根目录
  if (!fs.exists(Constant.UNMERGE_PATH)) {
    fs.mkdirs(Constant.UNMERGE_PATH)
  }
  if (!fs.exists(Constant.MERGED_PATH)) {
    fs.mkdirs(Constant.MERGED_PATH)
  }
  if (!fs.exists(Constant.MERGE_BY_HOUR)) {
    fs.mkdirs(Constant.MERGE_BY_HOUR)
  }
  //删除上一次未完成的文件
  fs.list(Constant.UNMERGE_PATH, _.toString.takeRight(Constant.TMP_SUFFIX.size) == Constant.TMP_SUFFIX).foreach(p => fs.delete(p.toString))
}
object ReceiveContext{
  def apply(stopFilePath: String,ssc: StreamingContext): ReceiveContext = new ReceiveContext(stopFilePath,ssc)
}