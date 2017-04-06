package spark

/**
  * Created by Administrator on 2017/3/21.
  */
import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\HADOOP\\")

    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext("local","wordcount",conf)
    val line = sc.textFile("D:\\Desktop\\wordcount.txt")
    val outPath = new PrintWriter("D:\\Desktop\\test")

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

//    outPath.print(line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).foreach(println))
    sc.stop()
  }
}
