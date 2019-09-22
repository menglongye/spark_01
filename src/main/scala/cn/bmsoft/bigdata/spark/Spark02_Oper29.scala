package cn.bmsoft.bigdata.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yml on 2019/9/20.
  */
object Spark02_Oper29 {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc: SparkContext = new SparkContext(sparkConf)
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,"phone_1")
    conf.set("hbase.zookeeper.quorum","192.168.247.128,192.168.247.129,192.168.247.130")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val hbaseRDd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
   hbaseRDd.foreach{
     case (rowkey,result)=>{
      val key: String = Bytes.toString(result.getRow)
      val username: String = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("username")))
      val password: String = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("password")))
       println("Rowkey="+key+username+password)


   }
   }



    sc.stop()
  }


}
