package com.lx.collecter.merge

import java.time.LocalTime

import com.lx.util.Constant

/**
  * 定义持久化文件的命名规则
  */
object FileName {


  val split = "-"

  def getFileName(createTime: String, batchId: String): String = {
    createTime + split + batchId + split + System.nanoTime() + Constant.TMP_SUFFIX
  }

  def getTime(fileName: String): String = {
    fileName.split(split)(0)
  }

}
