package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper26 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
    val json: RDD[String] = sc.textFile("in")
    val result: RDD[Option[Any]] = json.map(JSON.parseFull)
   result.foreach(println)


  }
}
