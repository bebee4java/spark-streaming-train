package org.spark.train

import scalikejdbc._


/**
  * 数据库操作相关工具
  *
  * @author sgr
  * @version 1.0, 2019-02-18 16:26
  **/
object DbUtils {
  private var driverClass:String = _
  private var url:String = _
  private var username:String = _
  private var password:String = _

  private var connectionPool:ConnectionPool = _


  def init(driverClass:String, url:String, username:String, password:String) = {
    this.driverClass = driverClass
    this.url = url
    this.username = username
    this.password = password

    Class.forName(driverClass)
    ConnectionPool.singleton(url, username, password)
    connectionPool = ConnectionPool.apply()
  }

  def exec_insert_sql(_sql:String) = {
    using(DB(this.connectionPool.borrow())) {
      db => {
        db.localTx {
          implicit session => {
            SQL(_sql).update().apply()
          }
        }
      }
    }
  }

  def exec_select_sql(_sql:String, columns:List[String]) = {
    using(DB(this.connectionPool.borrow())) {
      db => {
        db.localTx {
          implicit session => {
            SQL(_sql).map(records => {
              val record = new Array[String](columns.size)
              var i = 0
              for(column <- columns){
                record(i) = records.get(column)
                i+=1
              }
              record
            }).list().apply()
          }
        }
      }
    }
  }




  def main(args: Array[String]): Unit = {

    init("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test",
      "root", "253824")

    val connection = this.connectionPool.borrow()
    val words = using(DB(connection)) {
      db => {
        db.localTx {
          implicit session => {
            sql"select word from wordcount"
              .map(rs => {rs.string("word")}).list.apply()
          }
        }
      }
    }

    println(words)

    var sql:String = "insert into wordcount(word,count) values('test7', 100)"
    println(exec_insert_sql(sql))

    sql = "select * from wordcount"
    val result:List[Array[String]] = exec_select_sql(sql, List("word","count"))
    for (line <- result){
      println(line.mkString(","))
    }

  }

}
