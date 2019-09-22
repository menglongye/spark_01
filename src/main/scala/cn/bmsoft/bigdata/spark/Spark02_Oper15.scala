package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper15 {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val sordRDD: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"a"),(4,"c"),(6,"b"),(1,"c")))
    val value: RDD[(Int, String)] = sordRDD.sortByKey(true)
    value.collect().foreach(println)







  }
}
