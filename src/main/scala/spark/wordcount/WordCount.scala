package spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

object WordCount {

  def main(args: Array[String]): Unit = {

    /**
     * 第一步 获取编程入口
     */
    val config:SparkConf=new SparkConf()
    config.setAppName("WordCount")
    config.setMaster("local")
    val sparkContext:SparkContext=new SparkContext(config)
    sparkContext.setLogLevel("WARN")

    /*val array=Array(1,2,3,4,5,6,7,8,9,10)
    var rdd1:RDD[Int]=sparkContext.parallelize(array)
    var rdd2:RDD[Int]=sparkContext.makeRDD(array)*/

    /**
     * 第二步 通过变成入口，加载数据
     */
    val linesRDD:RDD[String]=sparkContext.textFile("file:/Users/barry.cao/Desktop/wordcount.txt")

    /**
     * 第三步 对数据进行逻辑处理
     */
    val wordAndCountRDD:RDD[(String,Int)]=linesRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

    /**
     * 第四步 对结果数据进行处理
     */
    wordAndCountRDD.foreach(x => println(x))

    /**
     * 第五步 关闭编程入口
     */
    sparkContext.stop()
  }
}
