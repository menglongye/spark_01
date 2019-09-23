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
    //转换为df
    val df: DataFrame = rdd.toDF("id","name","age")


    //转换为ds
    val ds: Dataset[User] = df.as[User]
    //转换为rdd
  val df1: DataFrame = ds.toDF()

    val rdd1: RDD[Row] = df1.rdd
        rdd1.foreach(
          row=>{
            //获取数据通过索引获取
            println(row.getString(1))
          }
        )
  }
}
case class User(id:Int,name:String,age:Int)