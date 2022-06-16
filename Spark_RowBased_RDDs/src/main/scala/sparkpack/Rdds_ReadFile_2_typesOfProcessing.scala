package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Rdds2_file_typesOfProcessing {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("third").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   
    val data = sc.textFile("file:///d:/Big data full stack/Spark_files/data/statecity.txt")
    data.foreach(println)
    println
    
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

  }
}