package org.spark.train

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 读取文件系统目录下的数据，这里不会启动Receiver，所以master可以为local[1]
  * 注意数据文件必须原子性move到监控目录下，文件放入即不能修改，修改形式的不再读取
  * 文件里的数据格式必须一致
  */
object LocalFileWC {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LocalFileWC").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val lines = ssc.textFileStream("file:///Users/sgr/Desktop/temp/")

    val wc = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    wc.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
