package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/5/15.
  */
object testSpark {

  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\HADOOP\\")
    val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
    val sc = new SparkContext(conf)
    val rddInt:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,2,5,1))
    println(rddInt.map(x => x + 1).collect().mkString(","))
    println(rddInt.countByValue())

  }


}
