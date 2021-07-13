package com.spark.btt.code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import java.time._

object sparkUDF {

  def toLower = (region: String) => {
    val arr = region.split(" ")
    arr.map(f => f.substring(0, 1).toLowerCase() + f.substring(1, f.length))
    .mkString("_")
  }

  def yearDiff(indpYear: Int): String = {
    val year = Year.now.getValue
    val p = Period.between(LocalDate.parse(indpYear + "-01-01"), LocalDate.parse(year + "-01-01"))
    //println(LocalDate.parse(indpYear+"-01-01")+"  => "+LocalDate.parse(year+"-01-01") + " => "+ p.getYears.toString())
    p.getYears.toString()
  }

  def main(args: Array[String]) {

    // Spark Session object
    val spark = SparkSession.builder()
      .appName("Create UDF")
      .master("local[*]")
      .getOrCreate();

    val df = spark.read.option("header", "true").csv(".\\sampleData\\countryData.csv")
      
    df.show(false)

    // Use UDF function in code
    val codeUDF = udf(toLower)
    df.select(col("Code"), col("Name"), col("Region"), codeUDF(col("Region")).as("Region_codeUDF")).show(false)

    // Use UDF function in spark sql
    val sqlUDF = udf[String, Int](yearDiff)
    spark.udf.register("sqlUDF", sqlUDF)
    // Create a temp view
    df.createOrReplaceTempView("TempTable")
    spark.sql("Select code,name,IndepYear,sqlUDF(IndepYear) as IndepYear_sqlUDF from TempTable").show(false)

    spark.close();
  }

}