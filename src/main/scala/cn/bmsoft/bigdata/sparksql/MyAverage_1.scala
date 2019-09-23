package cn.bmsoft.bigdata.sparksql


import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
  * Created by yml on 2019/9/23.
  */
object MyAverage_1 extends Aggregator[Person,Average,Double] {
  def main(args: Array[String]) {

  }

  override def zero: Average = {
    Average(0L, 0L)
  }

  override def reduce(buffer: Average, person: Person): Average = {
    buffer.sum += person.age
    buffer.count += 1
    buffer
  }

  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count


  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1

  }
    override def bufferEncoder: Encoder[Average]= Encoders.product

    override def outputEncoder: Encoder[Double]= Encoders.scalaDouble


}
  case class Person(name:String,age:Long)
  case class Average(var sum:Long,var count:Long)