package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object SeriTest {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SeriTest")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(Array("hadoop", "spark", "hive", "atguigu"))
    val search: Search = new Search("h")
  //val match1: RDD[String] = search.getMatch1(rdd)
    val match1: RDD[String] = search.getMatch2(rdd)
    match1.foreach(println)

    sc.stop()
  }
}

class Search(query:String)  {
  //过滤出包含字符串的数据
  def isMatch(s:String): Boolean ={
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd:RDD[String]): RDD[String] ={
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDDD
  def getMatch2(rdd:RDD[String]): RDD[String] ={
    var q=query
    rdd.filter(x=>x.contains(q))
  }
}
