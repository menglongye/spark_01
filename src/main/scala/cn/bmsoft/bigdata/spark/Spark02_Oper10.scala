package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper10 {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val coalesceRDD: RDD[Int] = sc.makeRDD(1 to 16,4)
    println( coalesceRDD.partitions.size)
    println("====")
    val listRdd: RDD[Int] = coalesceRDD.coalesce(3)
    println(listRdd.partitions.size)
    /*
    *20190919qq
    * */
    //distinctRDD.collect().foreach(println)


  }
}
