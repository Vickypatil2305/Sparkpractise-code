package selectexpr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SelectExprExpression {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val spark=SparkSession.builder().getOrCreate()
                  import spark.implicits._
                  
    val df=spark.read.format("csv").option("header","true").load("file:///d:/Big data full stack/Spark_files/data/txns_head1.txt")
//    df.show()
    val data=df.selectExpr("txnno",
                           "split(txndate,'-')[2] as txndate",
                           "custno",
                           "amount",
                           "category",
                           "product",
                           "city",
                           "state",
                           "spendby"
               )
//    data.show()          
//                                       Task 3
                  
     val dff=spark.read.format("csv").option("header","true").load("file:///d:/Big data full stack/Spark_files/data/txns_head1.txt")
     
     val year_check=dff.filter(col("category")==="Gymnastics")
               .selectExpr("txnno",
                           "split(txndate,'-')[2] as year",
                           "custno",
                           "amount",
                           "category",
                           "product",
                           "city",
                           "state",
                           "standby",
                           "case when spendby='cash' then 1 else 0 end as check"
               )     
     year_check.show()
          
  }
}