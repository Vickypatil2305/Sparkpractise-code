package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Dataframe1 {
  
  def main(args:Array[String]): Unit={
    
    val conf=new SparkConf().setAppName("Dataframe").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession.builder()
                          .appName("spark sql basis ex")
                          .config("spark.some.config.option","some-value")
                          .getOrCreate()
                          import spark.implicits._
    
    val df=spark.read.format("csv").option("header", "true")
    .load("file:///d:/Big data full stack/Spark_files/data/usdata.csv")
    df.show()
//    df.printSchema()
    
    df.createTempView("ustable")
    
    val filtercity=spark.sql("select * from ustable where city='Brighton'")
    filtercity.show()
    
     println("===gym===")
    val lastname = spark.sql("select * from ustable where last_name like '%Wie%' ")
    lastname.show()

    println("===email===")
    val email = spark.sql("select * from ustable where email like '%mi%' ")
    email.show()
    
    println("===age===")
    val age = spark.sql("select * from ustable where age like '%43%' ")
    age.show()
     
  }
}