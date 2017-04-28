package spark.sparksql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/28.
  */
object sparkSql {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\HADOOP\\")
    val conf = new SparkConf()
    val sc = new SparkContext("local","sparkSql",conf)
//   val line =  sc.textFile("D:\\Desktop\\testSparkSql.txt")
//    line.flatMap(_.split("#")).collect().foreach(println)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create the DataFrame
    val df = sqlContext.read.text("D:\\Desktop\\testSparkSql.txt")
//    df.select("name").show()
//    df.show()

  }

}
