package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper27 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(Array((1,2),(3,4),(5,6)))
    //写文件
    //rdd.saveAsSequenceFile("output")
//读文件
 val rdd1: RDD[(Int, Int)] = sc.sequenceFile[Int,Int]("output")
    rdd1.foreach(println)



  }
}
