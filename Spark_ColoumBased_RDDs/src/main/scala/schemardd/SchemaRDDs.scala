package schemardd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SchemaRDDs {

  case class schema(txnno: String, tdate: String, category: String, product: String)

  def main(args: Array[String]): Unit = {

    gymDataFilter();
  }

  def gymDataFilter() {

    val conf = new SparkConf().setAppName("Schemardds").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rddString = sc.textFile("file:///d:/Big data full stack/Spark_files/data/datat.txt", 1)
    rddString.foreach(println)
    println
    
    val splitdata = rddString.map(x => x.split(","))

    val schemardd = splitdata.map(x => schema(x(0), x(1), x(2), x(3)))
    schemardd.foreach(println)
    println
    
    val filltergym = schemardd.filter(x => x.product.contains("Gymnastics"))
    filltergym.foreach(println)
  }
}