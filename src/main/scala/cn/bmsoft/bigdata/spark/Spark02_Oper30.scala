package cn.bmsoft.bigdata.spark

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper30 {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("word","hive","hbse","spark","scala"),2)
    //创建累加器 来共享变量
   // val accumulator: LongAccumulator = sc.longAccumulator
//注册累加器

    val accumulator = new WordAccumulator
    sc.register(accumulator)

    rdd.foreach{
      case word =>{
        accumulator.add(word)
      }
    }
println("sum="+accumulator.value)


    sc.stop()
  }
}
//声明累加器

class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]]{

   val list = new util.ArrayList[String]()

  //当前的累加器是否是初始化状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator
  }
//重制累加器
  override def reset(): Unit ={
    list.clear()
  }
//向累加器增加数据
  override def add(v: String): Unit = {
    println("==="+v)
    if(v.contains("h")){
      list.add(v)

    }
  }
//合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }
//
  override def value: util.ArrayList[String] = {
    list
  }
}