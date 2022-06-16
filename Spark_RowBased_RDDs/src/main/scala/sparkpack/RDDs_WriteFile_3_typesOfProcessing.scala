package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object RDDs_WriteFile_3_typesOfProcessing {
  
 def main(args: Array[String]): Unit = {
   
   val conf=new SparkConf().setAppName("write").setMaster("local[*]")
   val sc=new SparkContext(conf)
   sc.setLogLevel("ERROR")
   
   val data=sc.textFile("file:///d:/Big data full stack/Spark_files/data/txns.txt")
   data.foreach(println)
   
    val flatten = data.flatMap(x => x.split("~"))

    val filterState = flatten.filter(x => x.contains("State->"))
    //    filterState.foreach(println)

    val filterCity = flatten.filter(x => x.contains("City->"))
    //    filtarCity.foreach(println)

    val stateList = filterState.map(x => x.replace("State->", ""))
    stateList.foreach(println)

    println()

    val cityList = filterCity.map(x => x.replace("City->", ""))
    cityList.foreach(println)

    //if it doesn't work,it is ok as of now because Winutils software must be installed
   stateList.saveAsTextFile("file:///d:/Big data full stack/Spark_files/data/rddswritefiledata")
   
   
 }
  
}