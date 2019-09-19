package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper12 {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val pairRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2)))
    val group: RDD[(String, Iterable[Int])] = pairRDD.groupByKey()
    group.collect().foreach(println)



  }
}
