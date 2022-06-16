package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object typesOfProcessing {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("First").setMaster("local[*]");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:///d:/Big data full stack/Spark_files/data/zeyodata.txt")
    val flatten = data.flatMap(x => x.split("~"))
    val filterList = flatten.filter(x => x.contains("B"))
    val replaceData=filterList.map(x=>x.replace("B"," Vicky"))
//    replaceData.foreach(println)
    
    val suffix=replaceData.map(x=>x + " ,")
    suffix.foreach(println)
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}