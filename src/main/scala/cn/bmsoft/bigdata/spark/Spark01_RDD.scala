package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //创建RDD
    //从内存中创建makeRDD
    //  val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //设定自己的分区
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //从内存中创建
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3))
    //外部存储中创建


    val fileRDD: RDD[String] = sc.textFile("on")

    listRDD.saveAsTextFile("output")


    listRDD.collect().foreach(println)



  }
}
