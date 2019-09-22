package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper23 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
      val key: Map[Int, Long] = rdd1.countByKey()

    println(key)

  }
}
