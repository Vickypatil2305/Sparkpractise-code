package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Rdd_ReadWrite {

  def main(args: Array[String]): Unit = {
    gymDataFilter()

  }

  def gymDataFilter() {

    val conf = new SparkConf().setAppName("third").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println

    val data = sc.textFile("file:///d:/Big data full stack/Spark_files/data/usdata.csv")

    println("====rawdata=====-")
    data.take(10).foreach(println)
    
    println("====lenghtData=====-")
    val lenghtData=data.filter(x=>x.length()>200)
    lenghtData.foreach(println)
    
    println
    println("====flatdata=====-")  
    val flatdata=lenghtData.flatMap(x=>x.split(","))
    flatdata.foreach(println)
  
    println
    val suffixData=flatdata.map(x=>x + "  , zeyo")
    suffixData.foreach(println)
    
    println
    val replaceHyphen=suffixData.map(x=>x.replace("-", ""))
    replaceHyphen.foreach(println)
    
    println
//    replaceHyphen.saveAsTextFile("file:///d:/data.txt")// it will not run because Winutils software must be installed
    println("====done=====-")
  
    
  }
}
  
