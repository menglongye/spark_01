package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper6{
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //map算子

 val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
  val value: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i=>i%2)

value.collect().foreach(println)


  }
}
