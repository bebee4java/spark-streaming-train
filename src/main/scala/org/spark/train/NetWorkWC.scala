package org.spark.train

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 读取远程socket流数据，这里会启动Receiver 所以master要用local至少要有2个线程及以上
  * 测试：
  * nc -lk 6789
  */

object NetWorkWC {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NetWorkWC").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost",6789)

    val wc = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    wc.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
