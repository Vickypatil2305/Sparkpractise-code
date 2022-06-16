package sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

object WithcolumnExx {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hfi").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/txns_head1.txt")

    val txndate = df.withColumn("txndate", expr("split(txndate,'-')[2]"))
    //  txndate.show()

    val year = df.withColumn("year", expr("split(txndate,'-')[2]"))
    //  year.show()

    val day_year = df.withColumn("day", expr("split(txndate,'-')[1]"))
                    .withColumn("year", expr("split(txndate,'-')[2]"))
//    day_year.show()
    
    val rename=df.withColumn("txndate",expr("split(txndate,'-')[2]"))
                  .withColumnRenamed("txndate", "year")
//    rename.show()
    
    val concate=df.withColumn("txndate", expr("concat(amount,' = ',category)"))
    concate.show()
    
    
//                      Hands ON - 1
    
    val df1 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/txns_head1.txt")
    val yearOnly=df1.withColumn("txndate", expr("split(txndate,'-')[2]"))
                    .withColumnRenamed("txndate", "year")
//    yearOnly.show()
  
//                      Hands ON - 2

    val df2 = spark.read.format("csv").option("header", "true").load("file:///d:/Big data full stack/Spark_files/data/txns_head1.txt")
    val filter=df2.filter(col("category")==="Gymnastics")
                  .withColumn("txndate", expr("split(txndate,'-')[2]"))
                  .withColumnRenamed("txndate", "year")  
                  .withColumn("check", expr("case when spendby='cash' then 0 else 1 end "))
    filter.show()
  
  
  
  
  
  
  }
}