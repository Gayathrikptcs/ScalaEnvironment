package com.ica

import com.ica.commons.Properties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import java.io.FileNotFoundException
import java.io.IOException

object Main {

  /**
   * The below function takes the arguments and creates the Dataframe.
   * Also clenses the data with no readings recorded.
   */
  def main(args: Array[String]): Unit = {
    
	// Logger to print only the error messages
    Logger.getLogger("org").setLevel(Level.ERROR)

    try {
	
	  // Creating the Spark session
      val spark = SparkSession
        .builder
        .appName("WeatherForecastAnalysis")
        .master("local[*]")
        .getOrCreate()

      //Temperature dataframe creation using input file
	  
      val tempDf = spark.read.format("csv")
        .option("delimiter", " ")
        .option("header", "true")
        .schema(Properties.temperatureSchema)
        .option("inferSchema", "true")
        .load("resources/temperature")

      //Data clensing - drops rows with empty readings
      val finalTempDf = tempDf.na.drop()

      //Final data saved in the specified path
      finalTempDf.write.parquet("resources/output/temperature")

    } catch {
      case ex: FileNotFoundException => {
        logger.error("File not found")
      }
      case ex: IOException => {
        logger.error("IO Exception")
      }
      finally
      {
        logger.error("Schema mismatched")
      }
    }
  }
}
