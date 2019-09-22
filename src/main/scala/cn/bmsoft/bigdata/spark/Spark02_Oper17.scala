package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper17 {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val value1: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))
    val value2: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(3,6)))
  val value: RDD[(Int, (String, Int))] = value1.join(value2)

    value.collect().foreach(println)







  }
}
