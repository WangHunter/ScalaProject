package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/3/23.
  */
object MapPartitions extends  App{

  System.setProperty("hadoop.home.dir", "D:\\HADOOP\\")

  val conf = new SparkConf().setMaster("local[2]").setAppName("mapPartitions")
  val sc = new SparkContext(conf)

  var rdd = sc.makeRDD(1 to 5,3)

  //rdd2将rdd中每个分区中的数值累加
  var rdd2 = rdd.mapPartitions{x => {
     var result = List[Int]()
         var i = 0
         while(x.hasNext){
             i += x.next()
           }
         result.::(i).iterator   //向队列的头部追加数据(x::list等价于list.::(x))
     }}

  rdd2.collect.foreach(println)
}
