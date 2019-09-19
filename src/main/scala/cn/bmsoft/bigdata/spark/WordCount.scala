package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //sc.textFile("/home/hadoop/input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect
    //1.创建SparkConf并设置App名称阿斯顿
  val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")

    //2.创建SparkContext，该对象是提交Spark App的入口
   val sc = new SparkContext(config)
    //println(sc)
  val lines:RDD[String] = sc.textFile("in")
  val words:RDD[String] = lines.flatMap(_.split(" "))
//为了统计方便，将单词的结构抓还
    val wordToOne:RDD[(String,Int)] = words.map((_,1))
val wordToSum:RDD[(String,Int)] = wordToOne.reduceByKey(_+_)

val result = wordToSum.collect()

    result.foreach(println)


  }
}
