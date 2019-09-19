package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper9 {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(List(5,2,3,4,5,1,2,3))
  //val distinctRDD: RDD[Int] = listRdd.distinct()
    //使用分区  
  val distinctRDD: RDD[Int] = listRdd.distinct(2)

    distinctRDD.collect().foreach(println)


  }
}
