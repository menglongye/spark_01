package cn.bmsoft.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by yml on 2019/9/23.
  */
object HelloWrod {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val spark: SparkSession = SparkSession.builder().appName("sparksql example").config(conf).getOrCreate()
   // val spark: SparkSession = new SparkSession(conf)
    import spark.implicits._
    val df: DataFrame = spark.read.json("in/people.json")
    df.show()
    df.filter($"age">10).show()
df.createOrReplaceTempView("persons")
    spark.sql("select * from persons where age >21").show()
    spark.stop()
  }
}
