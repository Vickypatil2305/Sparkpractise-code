package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rddfile {

  println("=================Started======================")

  def main(args: Array[String]): Unit = {
    gymDataFilter()
  
  }
  
  def gymDataFilter(){
    
    val conf = new SparkConf().setAppName("third").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println

    val data = sc.textFile("file:///d:/Big data full stack/Spark_files/data/datatxns.txt")
   
    println("====rawdata=====-")
    data.foreach(println)
    
    val gymdata = data.filter(x => x.contains("Gymnastics"))
   
    println("====processed data=====-")
    gymdata.foreach(println)
    
  }
}