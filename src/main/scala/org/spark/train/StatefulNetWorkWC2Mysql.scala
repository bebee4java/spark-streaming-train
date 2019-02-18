package org.spark.train

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 有状态的累计接收socket流数据单词统计结果入mysql
  *
  * @author sgr
  * @version 1.0, 2019-02-18 14:12
  **/
object StatefulNetWorkWC2Mysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulNetWorkWC2Mysql").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 5678)
    val dstream = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    /*
    该方式有问题 会报：
    org.apache.spark.SparkException: Task not serializable

    dstream.foreachRDD(rdd => {
      val connection = getConnection()  // executed at the driver
        rdd.foreach { record => {
          val word = record._1
          val count = record._2.toInt
          connection.createStatement().execute(s"insert into wordcount(word, count) values('$word', $count)")
        // executed at the worker
        }
      }
    })*/

    /*
    该方式有问题：
    每条记录都会创建一个连接

    dstream.foreachRDD(rdd => {
        rdd.foreach { record => {
          val connection = getConnection()
          val word = record._1
          val count = record._2.toInt
          connection.createStatement().execute(s"insert into wordcount(word, count) values('$word', $count)")
        // executed at the worker
        }
      }
    })*/

    /*dstream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords => {
          val connection = getConnection()
          partitionOfRecords.foreach(record => {
            val word = record._1
            val count = record._2.toInt
            val sql =
              s"""
                 |INSERT INTO `wordcount`(`word`, `count`) VALUES('$word', $count)
                 |  ON DUPLICATE KEY
                 |  UPDATE `count`=count+$count;
               """.stripMargin
            connection.createStatement().execute(sql)
            print("=======" + sql)
          })
          connection.close()
        }
      }
    }*/

    // 更效率的方式：连接池
    dstream.foreachRDD {rdd =>
      DbUtils.init("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
        "root", "253824")
      rdd.foreachPartition {
        partitionOfRecords => {
          partitionOfRecords.foreach(record => {
            val word = record._1
            val count = record._2.toInt
            val sql =
              s"""
                 |INSERT INTO `wordcount`(`word`, `count`) VALUES('$word', $count)
                 |  ON DUPLICATE KEY
                 |  UPDATE `count`=count+$count;
               """.stripMargin
            DbUtils.exec_insert_sql(sql)
            println("============")
          })

        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


  def getConnection(): Connection ={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
      "root", "253824")
  }

}
