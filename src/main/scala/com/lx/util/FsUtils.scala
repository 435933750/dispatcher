package com.lx.util
import java.io.{InputStream, OutputStream}
import java.net.URI
import com.lx.util.Constant._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
case class FsUtils(conf: Configuration = new Configuration()) {

  val logger = LoggerFactory.getLogger(FsUtils.getClass)
  conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
  conf.set("fs.defaultFS", "hdfs://ha-nn-uri")
  conf.set("dfs.nameservices", "ha-nn-uri")
  conf.set("dfs.ha.namenodes.ha-nn-uri", "nn1,nn2")
  conf.set("dfs.namenode.rpc-address.ha-nn-uri.nn1", "ip-10-0-6-232.eu-central-1.compute.internal:8020")
  conf.set("dfs.namenode.rpc-address.ha-nn-uri.nn2", "ip-10-0-6-201.eu-central-1.compute.internal:8020")
  conf.set("dfs.client.failover.proxy.provider.ha-nn-uri","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

  val fs = FileSystem.get(new URI(HDFS), conf, Constant.HDFS_USER);


  def rename(from: Path, to: Path) = {
    fs.rename(from, to)
  }

  def open(path: String): FSDataInputStream = {
    fs.open(new Path(path))
  }

  def open(path: Path): FSDataInputStream = {
    fs.open(path)
  }

  def mkdirs(path: String): Unit = {
    fs.mkdirs(new Path(path))
  }

  def mkdirs(path: Path): Unit = {
    fs.mkdirs(path)
  }

  def create(path: String): FSDataOutputStream = {
    fs.create(new Path(path), false)
  }

  def create(path: Path): FSDataOutputStream = {
    fs.create(path, false)
  }

  def delete(path: String): Unit = {
    fs.delete(new Path(path), true)
  }

  def exists(path: String) = {
    fs.exists(new Path(path))
  }

  def exists(path: Path) = {
    fs.exists(path)
  }

  def close(): Unit = {
    fs.close()
  }

  def list(path: String): ArrayBuffer[Path] = {
    val paths = ArrayBuffer[Path]()
    if (exists(path)) {
      val list = fs.listFiles(new Path(path), true)
      while (list.hasNext) {
        paths += list.next().getPath
      }
    }
    paths
  }

  def list(path: String, filter: (Path) => Boolean): ArrayBuffer[Path] = {
    val paths = ArrayBuffer[Path]()
    if (exists(path)) {
      val list = fs.listFiles(new Path(path), true)
      while (list.hasNext) {
        val path = list.next().getPath
        if (filter(path)) {
          paths += path
        }
      }
    }
    paths
  }

  def globStatus(path: String, filter: PathFilter = null) = {
    fs.globStatus(new Path(path), filter)
  }

  /**
    * 完成一个文件
    *
    * @param tmpPath
    * @param checkFile
    */
  def complate(tmpPath: String, checkFile: String => Boolean): Unit = {
    val x = fs.listFiles(new Path(tmpPath), true)
    while (x.hasNext) {
      val path = x.next().getPath
      val pathStr = path.toString
      if (checkFile(pathStr) && TMP_SUFFIX == pathStr.takeRight(TMP_SUFFIX.size)) {
        fs.rename(path, new Path(pathStr.dropRight(TMP_SUFFIX.size) + COMPLATE_SUFFIX))
      }
    }
  }

  /**
    * 压缩
    */
  def compress(from: String, target: String): Boolean = {
    var flag = false
    try {
      val fromPath = new Path(from)
      val targetPath = new Path(target)
      if (exists(fromPath) && (!exists(targetPath))) {
        val in = open(from)
        val out = create(targetPath)
        compressOrDecompress(in, out, conf)
        flag = true
      } else {
        logger.error(s"compress file check error:from=${from},target=${target}")
      }
    } catch {
      case e: Exception => {
        logger.error("decompress error", e)
      }
    }
    flag
  }

  /**
    * 压缩和解压
    *
    * @param in
    * @param out
    * @param conf
    * @param compress
    */
  def compressOrDecompress(in: InputStream, out: OutputStream, conf: Configuration, compress: Boolean = true,close:Boolean=true) = {
    val gzip = new GzipCodec()
    gzip.setConf(conf)
    val (from, to) = compress match {
      case true => (in, gzip.createOutputStream(out))
      case _ => (gzip.createInputStream(in), out)
    }
    IOUtils.copyBytes(from, to, conf, close)
  }
}
