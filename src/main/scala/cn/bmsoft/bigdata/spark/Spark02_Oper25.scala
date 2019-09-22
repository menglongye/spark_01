package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper25 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
val rdd: RDD[String] = sc.makeRDD(Array("bigdata"))
    val nocache: RDD[String] = rdd.map(_.toString+System.currentTimeMillis()).cache()
    nocache.foreach(println)
    nocache.foreach(println)
    nocache.foreach(println)


  }
}
