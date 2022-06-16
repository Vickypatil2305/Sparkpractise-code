package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Rdds1_typesOfProcessing {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("second").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val listStr = List(
      "State->Madhya Pradesh~City->Indore",
      "State->Maharashtra~City->Pune",
      "State->Karnatak~City->Bangloru",
      "State->Tamilnadu~City->Hydrabad",
      "State->Goa~City->Manik")

    val flatten = listStr.flatMap(x => x.split("~"))

    val filterState = flatten.filter(x => x.contains("State->"))
//    filterState.foreach(println)

    val filterCity = flatten.filter(x => x.contains("City->"))
//    filtarCity.foreach(println)
    
    val stateList=filterState.map(x=>x.replace("State->", ""))
    stateList.foreach(println)
    
     println()
    
    val cityList=filterCity.map(x=>x.replace("City->", ""))
    cityList.foreach(println)
    
    
    
    
  }

}