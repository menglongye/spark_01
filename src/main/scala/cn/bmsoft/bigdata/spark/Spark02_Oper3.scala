package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper3 {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //map算子

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)
    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, dates) => {
        dates.map((_, "分区好" + num))
      }
    }

    

    tupleRDD.collect().foreach(println)


   // mapPartitionsRDD.collect().foreach(print)




  }
}
