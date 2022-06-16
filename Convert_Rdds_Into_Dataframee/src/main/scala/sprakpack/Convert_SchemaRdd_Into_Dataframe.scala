package sprakpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Convert_SchemaRdd_Into_Dataframe {

  case class schema(txnno: String, tdate: String, category: String, product: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Schemardds").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark=SparkSession.builder().getOrCreate()
              import spark.implicits._
              
    val rddString = sc.textFile("file:///d:/Big data full stack/Spark_files/data/datat.txt", 1)
    rddString.foreach(println)
    println

    val splitdata = rddString.map(x => x.split(","))

    val schemardd = splitdata.map(x => schema(x(0), x(1), x(2), x(3)))
    schemardd.foreach(println)
    println

    val filtergym = schemardd.filter(x => x.product.contains("Gymnastics"))
    filtergym.foreach(println)
    
    println("==============")
        
    val df=filtergym.toDF()
    df.show()
    
//    df.write.parquet("file:///d:/pardata")
    println("======done========")
    
    
  }
}