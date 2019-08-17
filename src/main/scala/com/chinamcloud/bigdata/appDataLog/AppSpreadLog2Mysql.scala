package com.chinamcloud.bigdata.appDataLog

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/**
  * @Classname AppDataLog2Mysql 
  * @Description
  * 从kafka中读取数据到mysqln
  * @Date 2019/8/14 16:58 
  * @Created by yuhousheng
  */
object AppSpreadLog2Mysql {

  def main(args: Array[String]): Unit = {

    // spark streaming 初始配置
    val conf: SparkConf = new SparkConf()
      .setAppName("appDataLog")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context: StreamingContext = new StreamingContext(conf, Seconds(1))

    // kafka 配置
    val kafkaParamMap = Map(
        "bootstrap.servers" -> "server02:9092,server05:9092,server08:9092"
      , "group.id" -> "test"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "auto.offset.reset" -> "latest"
      , "enable.auto.commit" -> (false:java.lang.Boolean)
    )
    val topics = Array("app-spread-log")

    // 日志处理逻辑
    val valueStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      context,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topics, kafkaParamMap)
    )

    val value: DStream[String] = valueStream.map(_.value()) // 只要日志

    value.foreachRDD(rdd => { // 触发spark-streaming计算
      rdd.foreachPartition( x => {
        // 对日志进行业务逻辑操作
      })
    })

    context.start()
    context.awaitTermination()

  }

}
