package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper22 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(1 to 10 ,2)
    //val aggregate: Int = rdd1.aggregate(0)(_+_,_+_)
    val fold: Int = rdd1.fold(0)(_+_)

    println(fold)

  }
}
