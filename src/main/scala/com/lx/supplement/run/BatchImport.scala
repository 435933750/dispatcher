package com.lx.supplement.run

import com.lx.supplement.bean.SupplementBean
import com.lx.supplement.config.DataBaseConfig
import com.lx.supplement.run.BatchImport.execute
import com.lx.supplement.run.batchBean.{RcFactory, RcUser}
import com.lx.supplement.storage.SupplementStorage
import com.lx.util.{Constant, HBaseUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.immutable.Nil

/**
  * 离线批量导入hbase
  */

// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_user x false false
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_temp_user x false false
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_goddess x false false
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_user_pay_record x false false
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_live_chat_statistics.rc_user_praise_record x false false
//
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_user "2020-01-10" true true
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_temp_user "2020-01-10" true false
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_goddess "2020-01-10" true false
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_video_chat.rc_user_pay_record "2020-01-10" true true
// ./run-spark-batch.sh com.lx.supplement.run.BatchImport dispather-1.0-SNAPSHOT.jar rc_live_chat_statistics.rc_user_praise_record "2020-01-10" true true
// ./startSupple.sh
// ./startMonitor.sh
object BatchImport {

  def getSc(remote: Boolean = false): SparkContext = {
    val conf = new SparkConf().setAppName("BatchImport")
    if (!remote) {
      conf.setMaster("local")
    }
    new SparkContext(conf)
  }

  def getSs(remote: Boolean = false): SparkSession = {
    val conf = new SparkConf().setAppName("BatchImport")
    if (!remote) {
      conf.setMaster("local")
    }
    SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  }

  //只有批量导入rc_user表时，才不需要读取Hbase,其他表需要
  var readHbase = false

  def main(args: Array[String]): Unit = {
    args.foreach(a => println(s"args================${a}"))
    readHbase = args(3).toBoolean
    remote(args(0), args(1), args(2).toBoolean)
    //    local
//        local()
  }

  def local(): Unit = {
     //    spark.setLogLevel("WARN")
    //    val rdd = spark.parallelize(List(("", "{\"data\":[{\"gold_num\":\"300.0\",\"money\":\"2.99\",\"create_time\":\"2017-04-27 07:32:49.0\",\"user_id\":\"13134\"}],\"database\":\"rc_video_chat\",\"old\":null,\"table\":\"rc_user_pay_record\",\"ts\":1493278369000,\"type\":\"INSERT\"}")
    //      ,("","{\"data\":[{\"gold_num\":\"300.0\",\"money\":\"2.99\",\"create_time\":\"2017-04-13 05:36:27.0\",\"user_id\":\"13134\"}],\"database\":\"rc_video_chat\",\"old\":null,\"table\":\"rc_user_pay_record\",\"ts\":1492061787000,\"type\":\"INSERT\"}")))
    //    val json = "{\"data\":[{\"id\":\"11317538\",\"user_id\":\"64400941\",\"signture\":\"\",\"signtureData\":\"\",\"commodity_id\":\"19\",\"gold_num\":\"200\",\"money\":\"3.99000\",\"receipt_data\":null,\"recharge_id\":\"30000698818243\",\"transaction_id\":\"\",\"verify_result\":\"1\",\"user_device_id\":\"\",\"pay_platform\":\"2\",\"create_time\":\"2020-01-06 13:41:42\",\"verify_time\":\"2020-01-08 13:41:42\",\"google_order_id\":\"0\",\"app_id\":\"20000\",\"order_id\":null,\"event_id\":null}],\"database\":\"rc_video_chat\",\"es\":1578490902000,\"id\":11656229,\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"user_id\":\"int(11)\",\"signture\":\"varchar(500)\",\"signtureData\":\"varchar(500)\",\"commodity_id\":\"int(11)\",\"gold_num\":\"decimal(10,0)\",\"money\":\"decimal(10,5)\",\"receipt_data\":\"text\",\"recharge_id\":\"varchar(500)\",\"transaction_id\":\"varchar(500)\",\"verify_result\":\"tinyint(4)\",\"user_device_id\":\"varchar(50)\",\"pay_platform\":\"tinyint(4)\",\"create_time\":\"timestamp\",\"verify_time\":\"timestamp\",\"google_order_id\":\"varchar(50)\",\"app_id\":\"int(11)\",\"order_id\":\"varchar(128)\",\"event_id\":\"VARCHAR(64)\"},\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":4,\"user_id\":4,\"signture\":12,\"signtureData\":12,\"commodity_id\":4,\"gold_num\":3,\"money\":3,\"receipt_data\":-4,\"recharge_id\":12,\"transaction_id\":12,\"verify_result\":-6,\"user_device_id\":12,\"pay_platform\":-6,\"create_time\":93,\"verify_time\":93,\"google_order_id\":12,\"app_id\":4,\"order_id\":12,\"event_id\":12},\"table\":\"rc_user_pay_record\",\"ts\":1578490902477,\"type\":\"INSERT\"}"
    //    val json2 = "{\"data\":[{\"user_id\":\"12656715\",\"gold_num\":\"100\",\"money\":\"3.0\",\"create_time\":\"2020-01-08 12:00:00\"},{\"user_id\":\"12656715\",\"gold_num\":\"200\",\"money\":\"7.0\",\"create_time\":\"2020-01-08 13:00:00\"}],\"database\":\"rc_video_chat\",\"table\":\"rc_user_pay_record\",\"ts\":1578490902477,\"type\":\"INSERT\"}"
    //    val rdd = spark.parallelize(List(("", json2)))
    //    execute(rdd, spark)

    val spark = getSs()
    val bean = RcFactory.getBean("rc_video_chat.rc_user_pay_record", true)
    if (bean != null) {
      val url = bean.JDBC_URL + bean.mysqlDBName
      val sql = s"(select * from ${bean.mysqlTableName} where create_time>='2020-01-10' and user_id=64400941) as tb"
      println(s"sql=>>>>>>>>>>>>>>>>${sql}")
      val df = spark.read.format("jdbc").
        option("url", url).
        option("driver", Constant.JDBC_BUSINESS_DRIVER).
        option("dbtable", sql).
        option("user", Constant.JDBC_BUSINESS_USER_NAME).
        option("password", Constant.JDBC_BUSINESS_PASS_WORD).
        load()
      df.show()

      val rdd = df.rdd.map(r => ("", bean.rowToJson(r)))
      readHbase=true
      execute(rdd, spark.sparkContext)

    }
  }

  def remote(tableName: String, startTime: String, readMysql: Boolean = false) {
    if (!readMysql) {
      val bean = RcFactory.getBean(tableName)
      if (bean != null) {
        val spark = getSs(true)
        val query = if (tableName == "rc_video_chat.rc_user") {
          "user_id as id,*"
        } else {
          "*"
        }
        val sql = s"select ${query} from ${tableName}"
        val df = spark.sql(sql).repartition(if (!readHbase) 500 else 30)
        val rdd = df.rdd.map(r => ("", bean.rowToJson(r)))

        execute(rdd, spark.sparkContext)
      }
    } else {
      val spark = getSs(true)
      val bean = RcFactory.getBean(tableName, true)
      if (bean != null) {
        val url = bean.JDBC_URL + bean.mysqlDBName
        val sql = s"(select * from ${bean.mysqlTableName} where ${bean.filterCol} >= '${startTime} 00:00:00') as tb"
        println(s"sql=>>>>>>>>>>>>>>>>${sql}")
        val df = spark.read.format("jdbc").
          option("url", url).
          option("driver", Constant.JDBC_BUSINESS_DRIVER).
          option("dbtable", sql).
          option("user", Constant.JDBC_BUSINESS_USER_NAME).
          option("password", Constant.JDBC_BUSINESS_PASS_WORD).
          //option("numPartitions", 30).
          //option("partitionColumn", "id").
          //option("lowerBound", "0").
          //option("upperBound", "1000000").
          load()

        val rdd = df.rdd.map(r => ("", bean.rowToJson(r)))
        execute(rdd, spark.sparkContext)
      }
    }
  }


  def execute(rdd: RDD[(String, String)], spark: SparkContext): Unit = {

    val config = DataBaseConfig.createDatBaseConf()
    val configBroad = spark.broadcast(config.asScala.toMap)
    val readHbaseBrod = spark.broadcast[Boolean](readHbase)
    val store = SupplementStorage(configBroad, true)

    //把所有数据用 database+table组成key的分组,同一个表下的数据肯定在一个分区
    val group = rdd.mapPartitions(line => store.clearData((line, store.configBroad.value))).filter(x => (!x._1.isEmpty)).groupByKey()
    group.foreachPartition(p => {
      if (!p.isEmpty) {
        val list = p.toList
        val results: Array[Result] = if (!readHbaseBrod.value) {
          Array.empty[Result]
        } else {
          val pkSet = list.flatMap(_._2.toList).map(_.pk).toSet
          if (!pkSet.isEmpty) {
            val conn = HBaseUtil.getConn
            if (conn == null)
              throw new Exception("hbase connection is null")
            val outputTable = conn.getTable(TableName.valueOf("user_common_info"))
            val results = HBaseUtil.get(outputTable, pkSet.map(pk => {
              new Get(Bytes.toBytes(pk))
            }).toList: _*)
            outputTable.close()
            results
          } else {
            Nil.asInstanceOf[Array[Result]]
          }
        }

        val groupByOutoutTable = list.map(kv => {
          new SupplementBean(kv._2.toList, results).executor()
        }).flatten.groupBy(_.tableName)
        store.persist(groupByOutoutTable, "")
      }
    })
  }
}
