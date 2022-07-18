package com.ica.commons

import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
 * Property file to configure the schema and external properties
 */
object Properties {

  val temperatureSchema = new StructType()
    .add("year", IntegerType, nullable = true)
    .add("month", IntegerType, nullable = true)
    .add("day", IntegerType, nullable = true)
    .add("morning_observation", DoubleType, nullable = true)
    .add("noon_observation", DoubleType, nullable = true)
    .add("evening_observation", DoubleType, nullable = true)
    .add("tmax", DoubleType, nullable = true)
    .add("tmin", DoubleType, nullable = true)
    .add("tmean", DoubleType, nullable = true)

}
