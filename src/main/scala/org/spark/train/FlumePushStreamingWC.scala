package org.spark.train

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkstreaming 整合flume
  * 第一种方式：采用flume push-based
  * 这里会启动Receiver 所以master要用local至少要有2个线程及以上
  *
  * @author sgr
  * @version 1.0, 2019-02-20 00:22
  **/
object FlumePushStreamingWC {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("FlumePushStreamingWC").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setLogLevel("WARN")


    //TODO ... sparkstreaming 整合flume

    val flumeStream = FlumeUtils.createStream(ssc, "localhost", 6789) //sparkstreaming 会启动一个Avro agent for flume
    val wc = flumeStream.map(line => new String(line.event.getBody.array()).trim) //先拿到flume event 里的body信息转成string
        .flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)

    wc.print()


    ssc.start()
    ssc.awaitTermination()

  }

}
