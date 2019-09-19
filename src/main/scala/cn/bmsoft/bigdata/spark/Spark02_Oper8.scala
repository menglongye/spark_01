package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper8 {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
      //map算子
      val value: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8))
    val sampleRDD: RDD[Int] = value.sample(false,0.4,1)


    sampleRDD.collect().foreach(println)


  }
}
