package org.spark.train

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkstreaming 整合flume
  * 第二种方式：采用flume pull-based
  *
  * 采用sparksink sparkstreaming 会主动到sparksink里拉取数据
  * 这里会启动Receiver 所以master要用local至少要有2个线程及以上
  * Spark Streaming会使用一个可靠的Flume Receiver带有事务的把数据拉过来。
  * 事务仅在一下情况成功：数据被spark streaming接收到，而且以多副本方式保存。
  *
  * @author sgr
  * @version 1.0, 2019-02-20 01:05
  **/
object FlumePullStreamingWC {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FlumePullStreamingWC").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setLogLevel("WARN")

    val flumeStream = FlumeUtils.createPollingStream(ssc, "localhost", 6789)
    val wc = flumeStream.map(line => new String(line.event.getBody.array()).trim) //先拿到flume event 里的body信息转成string
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wc.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
