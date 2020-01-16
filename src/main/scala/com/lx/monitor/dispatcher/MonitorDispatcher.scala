package com.lx.monitor.dispatcher

import com.lx.monitor.bean.MonitorSchema
import com.lx.monitor.config.ReadSchemaConfig
import com.lx.supplement.bean.PersistBean
import com.lx.util.OffsetManager
import com.lx.{Dispatcher, RuntimeContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession, functions}
import org.apache.spark.streaming.dstream.InputDStream
import org.slf4j.LoggerFactory

import scala.collection.mutable

abstract class MonitorDispatcher(spark: SparkSession) extends Dispatcher[String, String, (Iterator[(String, String)], mutable.Map[String, mutable.Map[String, MonitorSchema]], List[String]), Iterator[Row], Map[String, List[PersistBean]]] {


  val logger = LoggerFactory.getLogger(classOf[MonitorDispatcher])

  /**
    * 处理数据入口
    *
    * @param ds
    */
  override def handleData(ds: InputDStream[(String, String)], kakfaOfs: OffsetManager, ctx: RuntimeContext): Unit = {


    val schemaMap = ReadSchemaConfig.getConfig()
    val titles = schemaMap.map(_._2.values).flatten.filter(_.display == 1).map(_.colName).toSet.toList.sorted
    val stp = StructType(titles.map(t => StructField(t, StringType)))

    val out = ds.transform(rdd => {
      rdd.mapPartitions(m => clearData(m, schemaMap, titles))
    })

    //    spark.sql("INSERT INTO default.test_table_partition partition(dt) SELECT   FROM test_table")

    out.foreachRDD(rdd => {
      try {

        val saveRDD = rdd.coalesce(1)
        spark.createDataFrame(saveRDD, stp).createOrReplaceTempView("tb")
//        spark.sql("select * from tb limit 10").show(false)
                spark.sql("select max(create_time) dt,now() from tb").show(false)
        spark.sql("INSERT INTO real_time_mysql_data.alarm_scheme_minute partition(dt) select create_time,user_id,register_time,platform_type,app_id,country_name,gender,new_user_flg,pay_status,frist_pay_flg,eroticism_behavior,channel,type,version,vip,sign_eroticism,role,thumbs,pay_money_sum,pay_gold_num_sum,status,gold_balance,remote_user_id,remote_register_time,remote_match_user_platform,remote_match_app_id,remote_match_user_country,remote_gender,remote_new_user_flg,remote_match_user_pay,remote_frist_pay_flg,remote_eroticism_behavior,remote_channel,remote_type,remote_version,remote_vip,remote_sign_eroticism,remote_role,remote_thumbs,remote_pay_money_sum,remote_pay_gold_num_sum,remote_status,remote_gold_balance,pay_platform,money,gold_num,is_friend,action_source,is_real_user,online_flg,comm,alarm_service_list,date_format(create_date,'yyyyMMddHH') create_date,to_date(create_time) dt from tb")
        //                  spark.sql("INSERT INTO real_time_mysql_data.alarm_scheme_minute partition(dt='2019-01-03') select create_time,user_id,register_time,platform_type,app_id,country_name,gender,new_user_flg,pay_status,frist_pay_flg,eroticism_behavior,channel,type,version,vip,sign_eroticism,role,tolerance,sexRate,thumbs,pay_money_sum,pay_gold_num_sum,status,gold_balance,remote_user_id,remote_register_time,remote_match_user_platform,remote_match_app_id,remote_match_user_country,remote_gender,remote_new_user_flg,remote_match_user_pay,remote_frist_pay_flg,remote_eroticism_behavior,remote_channel,remote_type,remote_version,remote_vip,remote_sign_eroticism,remote_role,remote_tolerance,remote_sexRate,remote_thumbs,remote_pay_money_sum,remote_pay_gold_num_sum,remote_status,remote_gold_balance,pay_platform,money,gold_num,is_friend,action_source,is_real_user,online_flg,comm,alarm_service_list,date_format(create_date,'yyyyMMddHH') create_date from tb")
        //                  val df = spark.createDataFrame(rdd, stp)
        //                  import spark.implicits._
        //                  val w =df.withColumn("create_date", functions.date_format($"create_date", "yyyyMMddHH"))
        //                  df.withColumn("dt", functions.to_date($"create_date")).
        //                  write.mode(SaveMode.Append).insertInto("real_time_mysql_data.alarm_scheme_minute")
        logger.warn("insert into success")
      } catch {
        case e: Exception => logger.error(s"insertError", e)
      }
    })


  }


}

