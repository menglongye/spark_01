package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper21 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
   val rdd1: RDD[Int] = sc.makeRDD(Array(1,24,2,5,6,7))
   // val take: Array[Int] = rdd1.take(2)
    val ordered: Array[Int] = rdd1.takeOrdered(3)
    println(ordered.mkString("+"))

  }
}
