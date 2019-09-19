package cn.bmsoft.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/19.
  */
object Spark02_Oper11 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8))

        val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val partionRDD: RDD[(String, Int)] = listRDD.partitionBy(new Mypartioner(3))
    partionRDD.saveAsTextFile("output")
  }
}
//声明分区奇

class Mypartioner(partions:Int) extends Partitioner{
  override def numPartitions: Int = {
    partions
  }

  override def getPartition(key: Any): Int = {
    1

  }
}