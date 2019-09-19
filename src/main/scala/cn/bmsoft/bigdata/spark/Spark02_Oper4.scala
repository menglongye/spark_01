package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper4 {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //map算子

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
    val value: RDD[Int] = listRDD.flatMap(dates=>dates)

    value.collect().foreach(println)


   // mapPartitionsRDD.collect().foreach(print)




  }
}
