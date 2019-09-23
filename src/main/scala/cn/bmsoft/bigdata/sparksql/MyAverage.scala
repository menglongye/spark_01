package cn.bmsoft.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by yml on 2019/9/23.
  */
object MyAverage extends UserDefinedAggregateFunction{
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val spark: SparkSession = SparkSession.builder().appName("sparksql example").config(conf).getOrCreate()
    import spark.implicits._
spark.udf.register("myAverage",MyAverage)
    val df: DataFrame = spark.read.json("in/people.json")
    df.createOrReplaceTempView("people")
    df.show()
val result: DataFrame = spark.sql("select myAverage(age) as aa from people")
    result.show()

  }
//聚合函数输入参数的数据类型
  override def inputSchema: StructType = {
  StructType(StructField("inputColumn", LongType) :: Nil)
  }
  // 聚合缓冲区中值得数据类型
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1

  }


   def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存工资的总额
    buffer(0) = 0L
    // 存工资的个数
    buffer(1) = 0L

  }

  override def deterministic: Boolean = true
  // 计算最终结果
  override def evaluate(buffer: Row): Double  = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
  // 返回值的数据类型
  override def dataType: DataType = {
    DoubleType
  }

  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
}
