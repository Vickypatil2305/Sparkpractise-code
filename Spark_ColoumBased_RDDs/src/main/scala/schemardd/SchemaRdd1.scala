package schemardd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ColumnBasedSchemaRdd {

  case class product(txnno:String,category:String,product:String)
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ColumnBasedSchemaRdd").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val readData=sc.textFile("file:///d:/Big data full stack/Spark_files/data/txnsmall.txt")
//    readData.foreach(println)
    
    val flatten=readData.map(x=>x.split(","))
    flatten.foreach(println) // it wont show proper result because it shows java data
    
    val schemardd=flatten.map(x=>product( x(0),x(1),x(2)  ))
    schemardd.foreach(println)
  }
}