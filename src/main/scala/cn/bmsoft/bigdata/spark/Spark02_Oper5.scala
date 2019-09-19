package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper5 {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //map算子

 val listRDD: RDD[Int] = sc.makeRDD(1 to 18,4)
    //将一个分区的数据放到数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()
   glomRDD.collect().foreach(array=>{
     println(array.mkString(","))
   })




  }
}
