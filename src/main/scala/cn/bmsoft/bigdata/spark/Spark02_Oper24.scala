package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper24 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
  val wordAndOne1: RDD[(String, Int)] = sc.textFile("in").flatMap(_.split("\t")).map((_,1))
    //println(wordAndOne1.toDebugString)
    println(wordAndOne1.dependencies)

    val wordAndCount1: RDD[(String, Int)] = wordAndOne1.reduceByKey(_+_)
    //println(wordAndCount1.toDebugString)
    println(wordAndCount1.dependencies)
    wordAndCount1.foreach(println)

  }
}
