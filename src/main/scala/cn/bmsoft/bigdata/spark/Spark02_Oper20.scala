package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper20 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
   //val rdd1: RDD[Int] = sc.makeRDD(1 to 10)
   val reduceRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
   // val collect: Array[Int] = rdd1.collect()
 // val count: Long = rdd1.count()
  //val first: Int = rdd1.first()
    val first: (String, Int) = reduceRDD.first()
    //println(collect.mkString("=="))
  println(first)
  }
}
