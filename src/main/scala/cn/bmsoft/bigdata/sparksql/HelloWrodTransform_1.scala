package cn.bmsoft.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by yml on 2019/9/23.
  */
object HelloWrodTransform {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val spark: SparkSession = SparkSession.builder().appName("sparksql example").config(conf).getOrCreate()
   // val spark: SparkSession = new SparkSession(conf)
    import spark.implicits._
    //val df: DataFrame = spark.read.json("in/people.json")
    //jiangdataframe转换城一张表
     //df.createOrReplaceTempView("person")
    //spark.sql("select * from person").show()
//创建rdd
val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))

    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val ds: Dataset[User] = userRDD.toDS()

val rdd1: RDD[User] = ds.rdd
    rdd.foreach(println)


  }
}
case class User(id:Int,name:String,age:Int)