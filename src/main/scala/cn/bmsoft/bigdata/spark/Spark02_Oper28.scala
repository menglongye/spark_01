package cn.bmsoft.bigdata.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper28 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/babasport"
    val userName = "root"
    val passWd = "123456"
//创建jdbcRDD 访问数据库
/*    val sql="select title,id from bbs_ad where id >? and id < ?"
  var jdbcRDD=  new JdbcRDD(sc,
      ()=>{
    null
        //获取数据库链接对象
        Class.forName(driver)
        DriverManager.getConnection(url,userName,passWd)

      },
    sql,
    1,
    48,
    2,
    (rs)=>{
  println(rs.getString(1)+","+rs.getInt(2))
    }
    )
    jdbcRDD.foreach(println)*/
    val rdd: RDD[(Int, String, String)] = sc.makeRDD(List((1,"a","1991-01-01"),(2,"b","1992-01-01")))
/*
    rdd.foreach{
      case (id,name,birthday)=>{
      Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url,userName,passWd)
        var sql ="insert into test_tb (id,name,birthday) values(?,?,?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setInt(1,id)
        statement.setString(2,name)
        statement.setString(3,birthday)
        statement.executeUpdate()

        statement.close()
        connection.close()
    }

    }
*/

    rdd.foreachPartition(dates=>{
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url,userName,passWd)
dates.foreach {
  case (id, name, birthday) => {
    var sql = "insert into test_tb (id,name,birthday) values(?,?,?)"
    val statement: PreparedStatement = connection.prepareStatement(sql)
    statement.setInt(1, id)
    statement.setString(2, name)
    statement.setString(3, birthday)
    statement.executeUpdate()
    statement.close()

  }

}
      connection.close()
    })


    sc.stop()
  }


}
