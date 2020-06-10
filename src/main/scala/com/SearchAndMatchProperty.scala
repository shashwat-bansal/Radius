package com

import java.io.FileInputStream
import java.util.Properties
import java.sql.DriverManager

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class Property(val id: String,
                    val latitude: Double,
                    val longitude: Double,
                    val price: Long,
                    val bedrooms: Int,
                    val bathrooms: Int,
                    val matchPercent: Int
                   )

object SearchAndMatchProperty {

  def getConnection(properties:Properties):Boolean={
    val url = "jdbc:mysql://" + properties.get("mySqlHost") + ":" + properties.getProperty("mySqlPort") + "/" +
      properties.get("dbName")
    var isValid = false
    val connection: java.sql.Connection = null
    try {
      val connection = DriverManager.getConnection(url, properties.getProperty("mySqlUser"), properties.getProperty("mySqlPassword"))
      isValid = connection.isValid(5)
    }
    catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    finally {
      if (connection != null)
        connection.close()
    }
    if (isValid) {
      println("Connection is established.")
      true
    }
    else {
      println("Bad connection or timeout.")
      false
    }
  }
  def getMatches(latitude:Double,longitude:Double, min_budget: Long, max_budget: Long, min_bedrooms: Int, max_bedrooms: Int, min_bathrooms: Int, max_bathrooms: Int, sqlContext:SQLContext,props: Properties) :List[Property] = {
    var result =  List.empty
    if(!getConnection(props)){
      println("Exiting...")
      result
    }
    val url = "jdbc:mysql://" + props.get("mySqlHost") + ":" + props.getProperty("mySqlPort") + "/" +
      props.get("dbName")
    val connectionProperties = new Properties()
    connectionProperties.put("user", props.getProperty("mySqlUser"))
    connectionProperties.put("password", props.getProperty("mySqlPassword"))
    connectionProperties.put("driver", "jdbc:mysql")
    val allPropertiesDF = sqlContext.read.jdbc(url, "PROPERTIES", connectionProperties)
    val mappedAllPropertiesDF=allPropertiesDF.map(row=>{

    })
    result
  }
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    val fStream = new FileInputStream(args(0))
    properties.load(fStream)
    fStream.close()
    val conf = new SparkConf()
      .setAppName(properties.getProperty("appName"))
      .setMaster(properties.getProperty("master"))
      .set("spark.ui.port", properties.getProperty("sparkPort"))
      .set("spark.sql.shuffle.partitions", properties.getProperty("partitions"))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("OFF")

}
}
