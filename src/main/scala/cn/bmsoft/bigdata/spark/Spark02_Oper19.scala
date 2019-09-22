package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper19 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
  //  val rdd1: RDD[Int] = sc.makeRDD(1 to 10,2)
 //   val reduce: Int = rdd1.reduce(_+_)
    val reduceRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
   // reduceRDD.reduce((x,y)=>(x._1 + y._1,x._2 + y._2))
    val tuple: (String, Int) = reduceRDD.reduce((x, y) => (
      x._1 + y._1, x._2 + y._2
      ))

    println(tuple)

  }
}
