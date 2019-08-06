package com.data.Retail

import org.apache.spark.sql.SparkSession
import java.lang.Float


object TotalSales {
  
  	def main(args: Array[String]) {
	  
		if (args.length < 2) {
      System.err.println("Usage: TotalSales <Input-File> <Output-File>");
      System.exit(1);
    }
		
		val spark = SparkSession
				.builder
				.appName("TotalSales")
				.getOrCreate()

    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.map { line => {
      val tokens = line.split("\\t")
      (Float.parseFloat(tokens(4)))
      }
    }
		
		val sum = result.sum()
		val count = result.count    
       
	spark.sparkContext.parallelize(Seq(sum,count),1).saveAsTextFile(args(1))
    
    spark.stop
  	}
}