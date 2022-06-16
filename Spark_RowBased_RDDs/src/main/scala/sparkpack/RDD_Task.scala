package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDD_Task {

  def main(args: Array[String]): Unit = {
    gymDataFilter()
  }
        
  def gymDataFilter() {

    val conf = new SparkConf().setAppName("third").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("==============================")
    println

    val data = sc.textFile("file:///d:/Big data full stack/Spark_files/data/txnss.txt")
    data.take(10).foreach(println)
    
    println("=============== Gym data================")

    val gymData = data.filter(x => x.contains("Gymnastics"))
    gymData.take(10).foreach(println)

    println("=============== Team Sports data================")

    val teamsportsData = data.filter(x => x.contains("Team Sports"))
    teamsportsData.take(10).foreach(println)
    
    println("=============== Combine Two RDDs ================")
    
    val merged=gymData.union(teamsportsData)
    merged.foreach(println)
  }
}