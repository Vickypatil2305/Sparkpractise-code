package sprakpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Convert_RowRdd_Into_Dataframe {

 /* Task 1
		1. Filter using ROW RDD 
		2. import org.apache.spark.sql.Row
		3. Define structtype
		4. Convert it to Dataframe 
		5. Show it
		
		Task 2 (Optional)
		1. write that dataframe as parquet
			 df.write.mode("overwrite").parquet("file:///C:/data/parqdata1")  -- If you face error then that could winUtils Issues 
	*/

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Schemardds").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession.builder().getOrCreate()
              import spark.implicits._

    println
		println("=====Raw data====")
    
    val rddString = sc.textFile("file:///d:/Big data full stack/Spark_files/data/datat.txt", 1)
    rddString.foreach(println)
    println

    println("=====row rdd data====")
    
    val splitdata = rddString.map(x => x.split(","))

    val rowrdd = splitdata.map(x => Row(x(0), x(1), x(2), x(3)))
//  rowrdd.foreach(println)
    println

    val filtergym = rowrdd.filter(x => x(3).toString().contains("Gymnastics"))
    filtergym.foreach(println)
            
    val schema=new StructType().add("txnno",StringType)
                               .add("tdate",StringType)
                               .add("category",StringType)
                               .add("product",StringType)
//    OR
    val schema1=new StructType(Array(StructField("id",StringType),
                                    StructField("tdate",StringType),
                                    StructField("category",StringType),
                                    StructField("product",StringType)))
   
    println("=============dataframe=============")
    val df=spark.createDataFrame(filtergym,schema)
    df.show()

//  df.write.mode("overwrite").parquet("file:///d:/pardata") -- If you face error,that could winUtils Issues
    println("======done========")
    
    
  }
}